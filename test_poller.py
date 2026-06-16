import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


firebase_admin_stub = types.ModuleType("firebase_admin")
firebase_admin_stub._apps = []
firebase_admin_stub.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin_stub.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin_stub)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin_stub.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin_stub.db)

chargepoint_stub = types.ModuleType("python_chargepoint")
chargepoint_stub.ChargePoint = object
exceptions_stub = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions_stub.ChargePointCommunicationException = ChargePointCommunicationException
exceptions_stub.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", chargepoint_stub)
sys.modules.setdefault("python_chargepoint.exceptions", exceptions_stub)

import poller


def utc_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class PollerTimerSafetyTests(unittest.TestCase):
    def test_reset_ignores_client_iso_and_does_not_crash_policy_enrichment(self):
        old_start = datetime.now(timezone.utc) - timedelta(hours=1)
        reset_ms = utc_ms(old_start + timedelta(minutes=20))
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}
        reset_map = {"101-1": {"at_ms": reset_ms, "at": "not-a-date"}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)
        started_at = stations["101"]["port_sessions"]["1"]["started_at"]

        self.assertEqual(started_at, poller._utc_iso_from_ms(reset_ms))
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_future_reset_is_ignored(self):
        old_start = datetime.now(timezone.utc) - timedelta(minutes=30)
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}
        reset_map = {
            "101-1": {
                "at_ms": utc_ms(datetime.now(timezone.utc) + timedelta(hours=1)),
                "at": "2099-01-01T00:00:00+00:00",
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"], old_start.isoformat()
        )

    def test_invalid_existing_started_at_self_heals(self):
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "definitely-not-iso"}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        started_at = stations["101"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "definitely-not-iso")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_extension_deadline_cannot_shorten_or_extend_implausibly_far(self):
        start = datetime.now(timezone.utc) - timedelta(hours=1)
        base = utc_ms(start) + 120 * 60 * 1000
        valid_extension = base + 30 * 60 * 1000
        slot = ("101", "1", start.isoformat(), 120)

        self.assertEqual(
            poller._policy_deadline_ms(*slot, {"101-1": {"until_ms": base - 1}}),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                *slot,
                {"101-1": {"until_ms": utc_ms(datetime.now(timezone.utc) + timedelta(days=30))}},
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(*slot, {"101-1": {"until_ms": valid_extension}}),
            valid_extension,
        )

    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        prev_root = {
            "101": {
                "device_id": 101,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-06-16T10:00:00+00:00"}},
                "updated_at": "2026-06-16T10:01:00+00:00",
            }
        }
        stations = {
            "101": {
                "error": "temporary ChargePoint failure",
                "updated_at": "2026-06-16T10:05:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["101"])
        self.assertEqual(stations["101"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"],
            "2026-06-16T10:00:00+00:00",
        )
        self.assertEqual(stations["101"]["last_fetch_error"], "temporary ChargePoint failure")


if __name__ == "__main__":
    unittest.main()
