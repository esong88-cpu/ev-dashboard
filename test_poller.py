import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
firebase_admin.initialize_app = lambda *args, **kwargs: None
sys.modules.setdefault("firebase_admin", firebase_admin)

chargepoint = types.ModuleType("python_chargepoint")
chargepoint.ChargePoint = object
sys.modules.setdefault("python_chargepoint", chargepoint)

chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


chargepoint_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
chargepoint_exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

import poller


def iso_from_ms(value):
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


class PollerHardeningTests(unittest.TestCase):
    def test_reset_uses_bounded_numeric_time_and_ignores_public_string(self):
        now_ms = poller._now_ms()
        old_start_ms = now_ms - 10 * 60 * 1000
        reset_ms = now_ms - 60 * 1000
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_from_ms(old_start_ms)}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"1-1": {"at_ms": reset_ms, "at": "not-a-date"}},
        )

        started_at = stations["1"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(started_at, iso_from_ms(reset_ms))
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        now_ms = poller._now_ms()
        old_start = iso_from_ms(now_ms - 10 * 60 * 1000)
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"1-1": {"at_ms": now_ms + 60 * 60 * 1000, "at": "future"}},
        )

        self.assertEqual(stations["1"]["port_sessions"]["1"]["started_at"], old_start)

    def test_invalid_existing_start_self_heals_without_crashing(self):
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        started_at = stations["1"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_extension_deadline_cannot_shorten_or_exceed_cap(self):
        now_ms = poller._now_ms()
        start_ms = now_ms - 3 * 60 * 60 * 1000
        base = start_ms + 120 * 60 * 1000
        started_at = iso_from_ms(start_ms)

        shortened = poller._policy_deadline_ms(
            "1", "1", started_at, 120, {"1-1": {"until_ms": base - 60 * 60 * 1000}}
        )
        extended = poller._policy_deadline_ms(
            "1",
            "1",
            started_at,
            120,
            {"1-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 60 * 60 * 1000}},
        )

        self.assertEqual(shortened, base)
        self.assertEqual(extended, base + poller.MAX_EXTENSION_MS)

    def test_fetch_errors_preserve_previous_ports_and_sessions(self):
        prev_root = {
            "1": {
                "device_id": 1,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
                "updated_at": "old",
            }
        }
        stations = {"1": {"error": "timeout", "updated_at": "new"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["1"])
        self.assertEqual(stations["1"]["last_fetch_error"], "timeout")
        self.assertEqual(stations["1"]["ports"], prev_root["1"]["ports"])
        self.assertEqual(stations["1"]["port_sessions"], prev_root["1"]["port_sessions"])
        self.assertEqual(stations["1"]["updated_at"], "new")

    def test_empty_port_payload_preserves_previous_ports_and_sessions(self):
        prev_root = {
            "1": {
                "device_id": 1,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
            }
        }
        stations = {"1": {"ports": {}, "updated_at": "new"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertEqual(stations["1"]["last_fetch_error"], "empty port payload")
        self.assertEqual(stations["1"]["ports"], prev_root["1"]["ports"])
        self.assertEqual(stations["1"]["port_sessions"], prev_root["1"]["port_sessions"])


if __name__ == "__main__":
    unittest.main()
