#!/usr/bin/env python3
"""
Fetch ChargePoint station status once and push to Firebase Realtime Database (/stations).
Intended for GitHub Actions on a schedule (cron); run locally by setting the same env vars.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import firebase_admin
from firebase_admin import credentials, db
from requests import codes

from python_chargepoint import ChargePoint
from python_chargepoint.exceptions import (
    ChargePointCommunicationException,
    ChargePointLoginError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)

RESET_FUTURE_SKEW_MS = 5 * 60 * 1000
MAX_EXTENSION_MS = 8 * 60 * 60 * 1000


def _parse_station_ids(raw: str) -> List[int]:
    ids: List[int] = []
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        ids.append(int(part))
    return ids


def _normalize_port_status(status: str, status_v2: str) -> str:
    """
    Map ChargePoint API strings to: Available | Charging | Complete.
    Unknown values are passed through in Title Case for visibility.
    """
    s = f"{status_v2 or ''} {status or ''}".strip().lower()
    if not s:
        return "Unknown"

    if any(
        k in s
        for k in (
            "finish",
            "complete",
            "done",
            "fully_charged",
            "stopped",
        )
    ):
        return "Complete"

    if any(
        k in s
        for k in (
            "charg",
            "in_use",
            "inuse",
            "occupied",
            "prepar",
            "suspend",
            "session",
            "active",
        )
    ):
        return "Charging"

    if any(k in s for k in ("available", "free", "idle", "ready")):
        return "Available"

    if any(k in s for k in ("fault", "offline", "unavailable", "unknown")):
        return "Unavailable"

    return (status_v2 or status or "Unknown").replace("_", " ").title()


def _collect_ports_from_station_json(data: Dict[str, Any]) -> List[Tuple[int, str]]:
    """Extract (outlet_number, normalized_status) from mapcache station info JSON."""
    out: List[Tuple[int, str]] = []

    ports_info = data.get("portsInfo") or data.get("ports_info") or {}
    port_list = ports_info.get("ports") or []

    for p in port_list:
        num = int(p.get("outletNumber") or p.get("outlet_number") or 0)
        st = str(p.get("status") or "")
        st2 = str(p.get("statusV2") or p.get("status_v2") or "")
        label = _normalize_port_status(st, st2)
        key = num if num else len(out) + 1
        out.append((key, label))

    if not out:
        for p in data.get("ports") or []:
            num = int(p.get("outletNumber") or p.get("outlet_number") or 0)
            st = str(p.get("status") or "")
            st2 = str(p.get("statusV2") or p.get("status_v2") or "")
            label = _normalize_port_status(st, st2)
            key = num if num else len(out) + 1
            out.append((key, label))

    return out


def _rtdb_map(obj: Any) -> Dict[str, Any]:
    """
    Firebase Realtime Database may return a list instead of a dict when keys look
    like a dense integer sequence. Normalize to string-keyed dict.

    If we write keys "1" and "2", RTDB can return [None, value1, value2].
    Preserve the actual array index as the key so previous port "1" still
    matches current port "1".
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return {str(k): v for k, v in obj.items()}
    if isinstance(obj, list):
        out: Dict[str, Any] = {}
        for i, v in enumerate(obj):
            if v is not None:
                out[str(i)] = v
        return out
    return {}


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    return float(value)


def _session_occupancy_bucket(label: str) -> str:
    """available vs still plugged / in use (charging or CP-reported complete)."""
    n = (label or "").strip().lower()
    if n == "available":
        return "available"
    return "occupied"


def enrich_stations_with_port_sessions(
    prev_root: Dict[str, Any], stations: Dict[str, Any], reset_map: Dict[str, Any]
) -> List[str]:
    """
    Attach port_sessions.started_at per port when occupied; clear when available.
    Returns /slot_extensions keys (deviceId-portId) to delete when a port frees up.
    """
    prev_root = prev_root or {}
    prev_stations: Dict[str, Any] = {
        k: v
        for k, v in prev_root.items()
        if k not in ("last_updated", "charging_limit_minutes") and isinstance(v, dict)
    }
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    now_ms = int(now.timestamp() * 1000)
    ext_clear: List[str] = []

    for sid, entry in list(stations.items()):
        if not isinstance(entry, dict) or entry.get("error"):
            continue
        prev_entry = prev_stations.get(sid) or {}
        if not isinstance(prev_entry, dict):
            prev_entry = {}
        prev_ports = _rtdb_map(prev_entry.get("ports"))
        prev_sess = _rtdb_map(prev_entry.get("port_sessions"))
        ports = _rtdb_map(entry.get("ports"))
        if not ports:
            continue

        new_sess: Dict[str, Dict[str, str]] = {}
        for pk, label in ports.items():
            pk_s = str(pk)
            label_s = str(label)
            prev_label = str(prev_ports.get(pk_s) or "")
            was_occ = _session_occupancy_bucket(prev_label) == "occupied"
            is_occ = _session_occupancy_bucket(label_s) == "occupied"

            if not is_occ:
                if was_occ or pk_s in prev_sess:
                    ext_clear.append(f"{sid}-{pk_s}")
                continue

            old_start = (prev_sess.get(pk_s) or {}).get("started_at")
            if not was_occ or not old_start:
                started_at = now_iso
            else:
                started_at = old_start
            start_ms = _iso_to_utc_ms(started_at)
            if start_ms <= 0:
                started_at = now_iso
                start_ms = now_ms

            reset = reset_map.get(f"{sid}-{pk_s}") if isinstance(reset_map, dict) else None
            if isinstance(reset, dict):
                reset_ms = _finite_number(reset.get("at_ms"))
                if (
                    reset_ms is not None
                    and reset_ms > start_ms
                    and reset_ms <= now_ms + RESET_FUTURE_SKEW_MS
                ):
                    started_at = datetime.fromtimestamp(
                        reset_ms / 1000, tz=timezone.utc
                    ).isoformat()

            new_sess[pk_s] = {"started_at": started_at}

        if new_sess:
            entry["port_sessions"] = new_sess
        elif "port_sessions" in entry:
            del entry["port_sessions"]

    return ext_clear


def _iso_to_utc_ms(iso: str) -> int:
    if not iso:
        return 0
    s = str(iso).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError, OverflowError):
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _policy_deadline_ms(
    sid: str,
    pk_s: str,
    started_at: str,
    limit_minutes: int,
    ext_map: Dict[str, Any],
) -> int:
    start_ms = _iso_to_utc_ms(started_at)
    if start_ms <= 0:
        return 0
    base = start_ms + limit_minutes * 60 * 1000
    slot_key = f"{sid}-{pk_s}"
    ext = ext_map.get(slot_key) if isinstance(ext_map, dict) else None
    if isinstance(ext, dict):
        um = _finite_number(ext.get("until_ms"))
        if um is not None and um > 0:
            return min(max(int(um), base), base + MAX_EXTENSION_MS)
    return base


def enrich_policy_complete_since(
    prev_root: Dict[str, Any],
    stations: Dict[str, Any],
    ext_map: Dict[str, Any],
    limit_minutes: int,
) -> None:
    """
    Set port_sessions[p].policy_complete_since (UTC ISO) the first time the co-op
    deadline is passed while still occupied; clear when back inside the window.
    Survives page reloads for “Complete · Xm” display.
    """
    prev_stations: Dict[str, Any] = {
        k: v
        for k, v in (prev_root or {}).items()
        if k not in ("last_updated", "charging_limit_minutes") and isinstance(v, dict)
    }
    now_iso = datetime.now(timezone.utc).isoformat()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    for sid, entry in list(stations.items()):
        if not isinstance(entry, dict) or entry.get("error"):
            continue
        prev_entry = prev_stations.get(sid) or {}
        prev_ps = _rtdb_map(prev_entry.get("port_sessions"))
        ports = _rtdb_map(entry.get("ports"))
        psessions = entry.get("port_sessions")
        psessions = _rtdb_map(psessions) if psessions is not None else {}
        if not psessions:
            continue

        for pk_s, sub in list(psessions.items()):
            if not isinstance(sub, dict):
                continue
            started = sub.get("started_at")
            if not started:
                continue
            label_s = str(ports.get(pk_s, "") or "")
            if _session_occupancy_bucket(label_s) != "occupied":
                continue
            deadline = _policy_deadline_ms(
                str(sid), str(pk_s), str(started), limit_minutes, ext_map
            )
            if deadline <= 0:
                continue
            prev_sub = prev_ps.get(pk_s) or {}
            if not isinstance(prev_sub, dict):
                prev_sub = {}
            if now_ms >= deadline:
                existing = prev_sub.get("policy_complete_since")
                sub = {**sub, "policy_complete_since": existing or now_iso}
            else:
                sub = {k: v for k, v in sub.items() if k != "policy_complete_since"}
            psessions[pk_s] = sub
        entry["port_sessions"] = psessions


def preserve_station_state_on_fetch_errors(
    prev_root: Dict[str, Any], stations: Dict[str, Any]
) -> None:
    prev_stations: Dict[str, Any] = {
        k: v
        for k, v in (prev_root or {}).items()
        if k not in ("last_updated", "charging_limit_minutes") and isinstance(v, dict)
    }
    for sid, entry in list(stations.items()):
        if not isinstance(entry, dict):
            continue
        has_error = bool(entry.get("error"))
        if not has_error and _rtdb_map(entry.get("ports")):
            continue
        prev_entry = prev_stations.get(sid) or {}
        if not isinstance(prev_entry, dict):
            continue
        prev_ports = _rtdb_map(prev_entry.get("ports"))
        if not prev_ports:
            continue

        preserved = dict(prev_entry)
        preserved["ports"] = prev_ports
        prev_sessions = _rtdb_map(prev_entry.get("port_sessions"))
        if prev_sessions:
            preserved["port_sessions"] = prev_sessions
        elif "port_sessions" in preserved:
            del preserved["port_sessions"]
        preserved["updated_at"] = entry.get("updated_at") or datetime.now(timezone.utc).isoformat()
        preserved["last_fetch_error"] = (
            str(entry.get("error")) if has_error else "Station payload did not include ports"
        )
        preserved["stale"] = True
        stations[sid] = preserved


def fetch_public_station(
    client: ChargePoint, station_id: int
) -> Dict[str, Any]:
    """GET mapcache v3/station/info (same endpoint as python-chargepoint v2+)."""
    base = client.global_config.endpoints.mapcache.rstrip("/")
    url = f"{base}/v3/station/info"
    resp = client.session.get(
        url,
        params={"deviceId": str(station_id), "use_cache": "false"},
        timeout=60,
    )
    if resp.status_code != codes.ok:
        raise ChargePointCommunicationException(
            response=resp,
            message=f"Station info failed for device {station_id}: HTTP {resp.status_code}",
        )
    return resp.json()


def fetch_home_charger_status(
    client: ChargePoint, device_id: int
) -> Dict[str, Any]:
    """Home Flex / Panda status — single logical port from charging_status."""
    hs = client.get_home_charger_status(device_id)
    raw = hs.charging_status.upper()
    if raw == "AVAILABLE":
        label = "Available"
    elif raw == "CHARGING":
        label = "Charging"
    elif raw == "NOT_CHARGING":
        label = "Available"
    else:
        label = _normalize_port_status(raw, "")
    name_parts = [hs.brand or "", hs.model or ""]
    name = " ".join(x for x in name_parts if x).strip() or f"HomeCharger-{device_id}"
    return {
        "device_id": device_id,
        "name": [name],
        "source": "home_charger",
        "ports": {"1": label},
    }


def build_station_payload(
    client: ChargePoint, station_id: int, home_ids: set
) -> Dict[str, Any]:
    # Home Flex chargers use the panda/mobile status API; public posts use mapcache.
    if station_id in home_ids:
        data = fetch_home_charger_status(client, station_id)
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        return data

    raw = fetch_public_station(client, station_id)

    ports_map: Dict[str, str] = {}
    for outlet_num, label in _collect_ports_from_station_json(raw):
        key = str(outlet_num if outlet_num else len(ports_map) + 1)
        ports_map[key] = label

    name = raw.get("name") or []
    if isinstance(name, str):
        name = [name]

    return {
        "device_id": station_id,
        "name": name,
        "station_status": raw.get("stationStatus") or raw.get("station_status"),
        "ports": ports_map,
        "source": "public",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def init_firebase(database_url: str) -> None:
    if not database_url:
        raise SystemExit("FIREBASE_DATABASE_URL is required for Realtime Database.")

    if firebase_admin._apps:
        return

    key_raw = (os.getenv("FIREBASE_KEY") or "").strip()
    if key_raw:
        try:
            info = json.loads(key_raw)
        except json.JSONDecodeError as exc:
            raise SystemExit("FIREBASE_KEY must be valid JSON (service account).") from exc
        cred = credentials.Certificate(info)
    else:
        path = (os.getenv("FIREBASE_CREDENTIALS_PATH") or "firebase-key.json").strip()
        if not os.path.isfile(path):
            raise SystemExit(
                "Set FIREBASE_KEY to the service account JSON string, "
                "or set FIREBASE_CREDENTIALS_PATH to an existing key file."
            )
        cred = credentials.Certificate(path)

    firebase_admin.initialize_app(cred, {"databaseURL": database_url})


def poll_once(client: ChargePoint, station_ids: List[int], home_ids: set) -> Dict[str, Any]:
    stations: Dict[str, Any] = {}
    for sid in station_ids:
        try:
            stations[str(sid)] = build_station_payload(client, sid, home_ids)
        except Exception as exc:
            logger.exception("Failed to fetch station %s: %s", sid, exc)
            stations[str(sid)] = {
                "error": str(exc),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
    return stations


def main() -> None:
    # GitHub Actions / secrets (primary)
    username = (os.getenv("CHARGEPOINT_USER") or "").strip()
    password = os.getenv("CHARGEPOINT_PASS") or ""
    session_token = (os.getenv("CHARGEPOINT_SESSION_TOKEN") or "").strip()
    station_raw = (os.getenv("CHARGEPOINT_STATION_IDS") or "").strip()
    database_url = (os.getenv("FIREBASE_DATABASE_URL") or "").strip()

    if not username:
        logger.error("Set CHARGEPOINT_USER (ChargePoint login email or username).")
        sys.exit(1)
    if not password and not session_token:
        logger.error(
            "Set CHARGEPOINT_PASS and/or CHARGEPOINT_SESSION_TOKEN "
            "(session cookie if you use SSO / 2FA)."
        )
        sys.exit(1)
    if not station_raw:
        logger.error("Set CHARGEPOINT_STATION_IDS (comma-separated device IDs).")
        sys.exit(1)

    station_ids = _parse_station_ids(station_raw)
    init_firebase(database_url)

    logger.info("Logging in to ChargePoint…")
    try:
        client = ChargePoint(
            username,
            password if password else "unused",
            session_token=session_token,
        )
    except ChargePointLoginError as exc:
        resp = exc.args[0] if exc.args and hasattr(exc.args[0], "text") else None
        err_text = (getattr(resp, "text", None) or str(exc)) if resp is not None else str(exc)
        if "403" in err_text or "captcha" in err_text.lower() or "ad blocker" in err_text.lower():
            logger.error(
                "ChargePoint returned a bot-protection page (403 / captcha). "
                "Password login from cloud IPs (e.g. GitHub Actions) is often blocked. "
                "Fix: set secret CHARGEPOINT_SESSION_TOKEN to a valid browser session token "
                "(see .env.example); the poller uses it without hitting the login endpoint. "
                "Refresh the token periodically when API calls start failing."
            )
        raise

    try:
        home_ids = set(client.get_home_chargers())
    except Exception as exc:
        logger.warning("Could not list home chargers: %s", exc)
        home_ids = set()

    ref = db.reference("/stations")
    prev_root = ref.get() or {}
    payload = poll_once(client, station_ids, home_ids)
    preserve_station_state_on_fetch_errors(prev_root, payload)
    reset_map = db.reference("/slot_resets").get() or {}
    if not isinstance(reset_map, dict):
        reset_map = {}
    cleared_ext_keys = enrich_stations_with_port_sessions(prev_root, payload, reset_map)
    limit_raw = (os.getenv("CHARGING_LIMIT_MINUTES") or "120").strip()
    try:
        charging_limit_minutes = max(1, int(limit_raw))
    except ValueError:
        charging_limit_minutes = 120

    ext_map = db.reference("/slot_extensions").get() or {}
    if not isinstance(ext_map, dict):
        ext_map = {}
    enrich_policy_complete_since(prev_root, payload, ext_map, charging_limit_minutes)

    last_updated = datetime.now(timezone.utc).isoformat()
    root_payload: Dict[str, Any] = {
        "last_updated": last_updated,
        "charging_limit_minutes": charging_limit_minutes,
        **payload,
    }
    logger.info("Writing %d station(s) to Firebase…", len(payload))
    ref.set(root_payload)

    if cleared_ext_keys:
        ext_root = db.reference("/slot_extensions")
        reset_root = db.reference("/slot_resets")
        for key in dict.fromkeys(cleared_ext_keys):
            try:
                # null value in update removes the child (works across firebase-admin versions).
                ext_root.update({key: None})
                reset_root.update({key: None})
            except Exception as exc:
                logger.warning("Could not clear per-slot metadata for %s: %s", key, exc)
    logger.info(
        "Updated /stations (last_updated=%s): %s",
        last_updated,
        json.dumps(payload, default=str)[:500],
    )
    logger.info("Done.")


if __name__ == "__main__":
    main()
