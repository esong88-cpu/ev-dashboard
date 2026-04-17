#!/usr/bin/env python3
"""
Fetch ChargePoint station status once and push to Firebase Realtime Database (/stations).
Intended for GitHub Actions on a schedule (cron); run locally by setting the same env vars.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import firebase_admin
from firebase_admin import credentials, db
from requests import codes

from python_chargepoint import ChargePoint
from python_chargepoint.exceptions import ChargePointCommunicationException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


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
    client = ChargePoint(
        username,
        password if password else "unused",
        session_token=session_token,
    )

    try:
        home_ids = set(client.get_home_chargers())
    except Exception as exc:
        logger.warning("Could not list home chargers: %s", exc)
        home_ids = set()

    ref = db.reference("/stations")
    payload = poll_once(client, station_ids, home_ids)
    logger.info("Writing %d station(s) to Firebase…", len(payload))
    ref.set(payload)
    logger.info("Updated /stations: %s", json.dumps(payload, default=str)[:500])
    logger.info("Done.")


if __name__ == "__main__":
    main()
