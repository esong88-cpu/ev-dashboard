import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
firebase_admin.initialize_app = lambda cred, options: None
sys.modules.setdefault("firebase_admin", firebase_admin)

chargepoint_module = types.ModuleType("python_chargepoint")
chargepoint_module.ChargePoint = object
sys.modules.setdefault("python_chargepoint", chargepoint_module)

exceptions_module = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions_module.ChargePointCommunicationException = ChargePointCommunicationException
exceptions_module.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint.exceptions", exceptions_module)

import poller


def iso_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class PollerCorrectnessTests(unittest.TestCase):
    def test_rtdb_map_preserves_array_indexes(self):
        self.assertEqual(
            poller._rtdb_map([None, "Charging", "Available"]),
            {"1": "Charging", "2": "Available"},
        )

    def test_preserve_previous_station_on_error_keeps_session_clock(self):
        started_at = "2026-05-21T08:00:00+00:00"
        prev_root = {
            "last_updated": "2026-05-21T08:05:00+00:00",
            "charging_limit_minutes": 120,
            "101": {
                "device_id": 101,
                "name": ["Samtec 01"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started_at}},
                "updated_at": "2026-05-21T08:05:00+00:00",
            },
        }
        stations = {
            "101": {
                "error": "timeout",
                "updated_at": "2026-05-21T08:10:00+00:00",
            }
        }

        poller.preserve_previous_station_on_errors(prev_root, stations)
        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual(cleared, [])
        self.assertNotIn("error", stations["101"])
        self.assertEqual(stations["101"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"],
            started_at,
        )
        self.assertEqual(stations["101"]["last_fetch_error"], "timeout")
        self.assertEqual(
            stations["101"]["last_fetch_error_at"],
            "2026-05-21T08:10:00+00:00",
        )

    def test_reset_uses_bounded_millis_not_client_iso(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = now_ms - 60 * 60 * 1000
        reset_ms = now_ms - 30 * 60 * 1000
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_from_ms(start_ms)}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"101-1": {"at": "not-a-date", "at_ms": reset_ms}},
        )

        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"],
            iso_from_ms(reset_ms),
        )

    def test_reset_ignores_far_future_millis(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = now_ms - 60 * 60 * 1000
        future_ms = now_ms + poller.MAX_CLIENT_CLOCK_SKEW_MS + 60 * 1000
        start_iso = iso_from_ms(start_ms)
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": start_iso}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"101-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": future_ms}},
        )

        self.assertEqual(stations["101"]["port_sessions"]["1"]["started_at"], start_iso)

    def test_invalid_iso_returns_zero_instead_of_crashing(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)

    def test_extension_deadline_is_capped(self):
        started_at = "2026-05-21T08:00:00+00:00"
        start_ms = poller._iso_to_utc_ms(started_at)
        base = start_ms + 120 * 60 * 1000
        requested = base + 365 * 24 * 60 * 60 * 1000

        deadline = poller._policy_deadline_ms(
            "101",
            "1",
            started_at,
            120,
            {"101-1": {"until_ms": requested}},
        )

        self.assertEqual(deadline, base + poller.MAX_EXTENSION_MINUTES * 60 * 1000)


if __name__ == "__main__":
    unittest.main()
