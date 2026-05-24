import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
credentials = types.ModuleType("firebase_admin.credentials")
credentials.Certificate = lambda info: info
db = types.ModuleType("firebase_admin.db")
db.reference = lambda path: None
firebase_admin.credentials = credentials
firebase_admin.db = db
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", credentials)
sys.modules.setdefault("firebase_admin.db", db)

chargepoint = types.ModuleType("python_chargepoint")
chargepoint.ChargePoint = object
chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


chargepoint_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
chargepoint_exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", chargepoint)
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

import poller


class PollerCriticalBehaviorTest(unittest.TestCase):
    def test_failed_fetch_preserves_previous_station_payload(self):
        prev_root = {
            "last_updated": "2026-05-24T10:00:00+00:00",
            "123": {
                "device_id": 123,
                "name": ["Main Lot"],
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-05-24T09:00:00+00:00"}
                },
            },
        }
        stations = {
            "123": {
                "error": "timeout",
                "updated_at": "2026-05-24T11:00:00+00:00",
            }
        }

        poller.preserve_previous_station_state(prev_root, stations)

        self.assertEqual(stations["123"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-05-24T09:00:00+00:00",
        )
        self.assertNotIn("error", stations["123"])
        self.assertEqual(stations["123"]["last_fetch_error"], "timeout")

    def test_empty_ports_preserves_previous_station_payload(self):
        prev_root = {
            "123": {
                "device_id": 123,
                "name": ["Main Lot"],
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-05-24T09:00:00+00:00"}
                },
            },
        }
        stations = {
            "123": {
                "device_id": 123,
                "name": ["Main Lot"],
                "ports": {},
                "updated_at": "2026-05-24T11:00:00+00:00",
            }
        }

        poller.preserve_previous_station_state(prev_root, stations)

        self.assertEqual(stations["123"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["123"]["last_fetch_error"],
            "Station payload contained no ports",
        )

    def test_invalid_reset_iso_falls_back_to_valid_epoch_timestamp(self):
        old_start = "2026-05-24T09:00:00+00:00"
        reset_ms = int(
            datetime(2026, 5, 24, 10, 0, tzinfo=timezone.utc).timestamp() * 1000
        )
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {"123-1": {"at": "not-a-date", "at_ms": reset_ms}}

        poller.enrich_stations_with_port_sessions(
            {"123": {"ports": {"1": "Charging"}, "port_sessions": {"1": {"started_at": old_start}}}},
            stations,
            reset_map,
        )

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-05-24T10:00:00+00:00",
        )

    def test_invalid_iso_does_not_crash_policy_deadline(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)
        self.assertEqual(
            poller._policy_deadline_ms("123", "1", "not-a-date", 120, {}),
            0,
        )

    def test_required_station_ids_rejects_comma_only_value(self):
        with self.assertRaises(ValueError):
            poller._parse_required_station_ids(" , , ")


if __name__ == "__main__":
    unittest.main()
