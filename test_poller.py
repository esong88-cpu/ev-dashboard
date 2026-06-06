import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_import_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda value: value

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda path: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    firebase_admin.initialize_app = lambda cred, options=None: None

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = object

    cp_exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    cp_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    cp_exceptions.ChargePointLoginError = ChargePointLoginError

    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", credentials)
    sys.modules.setdefault("firebase_admin.db", db)
    sys.modules.setdefault("requests", requests)
    sys.modules.setdefault("python_chargepoint", chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", cp_exceptions)


_install_import_stubs()

import poller  # noqa: E402


class PollerSessionHardeningTest(unittest.TestCase):
    def test_transient_fetch_error_preserves_previous_station_timers(self):
        started_at = "2026-06-06T08:00:00+00:00"
        complete_since = "2026-06-06T10:00:00+00:00"
        prev_root = {
            "100": {
                "device_id": 100,
                "name": ["Station 100"],
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": started_at,
                        "policy_complete_since": complete_since,
                    }
                },
            }
        }
        stations = {
            "100": {
                "error": "timeout",
                "updated_at": "2026-06-06T11:00:00+00:00",
            }
        }

        poller.preserve_previous_station_state_on_errors(prev_root, stations)
        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual(cleared, [])
        self.assertNotIn("error", stations["100"])
        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            started_at,
        )
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["policy_complete_since"],
            complete_since,
        )
        self.assertEqual(stations["100"]["last_fetch_error"], "timeout")

    def test_reset_uses_bounded_numeric_timestamp_not_public_iso_string(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        reset_ms = int(
            (datetime.now(timezone.utc) - timedelta(seconds=1)).timestamp() * 1000
        )
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}
        reset_map = {"100-1": {"at": "not-an-iso-date", "at_ms": reset_ms}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        started_at = stations["100"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

    def test_far_future_reset_is_ignored(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        future_ms = int(
            (
                datetime.now(timezone.utc)
                + timedelta(milliseconds=poller.PUBLIC_CLOCK_SKEW_MS, hours=1)
            ).timestamp()
            * 1000
        )
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}
        reset_map = {"100-1": {"at": "2999-01-01T00:00:00+00:00", "at_ms": future_ms}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            old_start,
        )

    def test_extension_deadline_cannot_shorten_or_extend_implausibly_far(self):
        started_at = datetime.now(timezone.utc).isoformat()
        base = poller._iso_to_utc_ms(started_at) + 120 * 60 * 1000
        valid_until = base + 30 * 60 * 1000
        too_far = base + poller.MAX_EXTENSION_MS + 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", started_at, 120, {"100-1": {"until_ms": base - 1}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", started_at, 120, {"100-1": {"until_ms": too_far}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", started_at, 120, {"100-1": {"until_ms": valid_until}}
            ),
            valid_until,
        )

    def test_invalid_started_at_does_not_crash_policy_enrichment(self):
        stations = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-an-iso-date"}},
            }
        }

        poller.enrich_policy_complete_since({}, stations, {}, 120)

        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            "not-an-iso-date",
        )
        self.assertNotIn("policy_complete_since", stations["100"]["port_sessions"]["1"])


if __name__ == "__main__":
    unittest.main()
