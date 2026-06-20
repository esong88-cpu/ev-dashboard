import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.initialize_app = lambda *args, **kwargs: None
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules["firebase_admin"] = firebase_admin

chargepoint = types.ModuleType("python_chargepoint")


class ChargePoint:
    pass


chargepoint.ChargePoint = ChargePoint
sys.modules["python_chargepoint"] = chargepoint

exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions.ChargePointCommunicationException = ChargePointCommunicationException
exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules["python_chargepoint.exceptions"] = exceptions

import poller  # noqa: E402


def ms_from_now(delta: timedelta) -> int:
    return int((datetime.now(timezone.utc) + delta).timestamp() * 1000)


class TimerMetadataTests(unittest.TestCase):
    def test_reset_uses_bounded_at_ms_not_untrusted_iso_text(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        reset_ms = ms_from_now(timedelta(minutes=-5))
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            {"123": {"ports": {"1": "Charging"}, "port_sessions": {"1": {"started_at": old_start}}}},
            stations,
            {"123-1": {"at_ms": reset_ms, "at": "not-a-date"}},
        )

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "not-a-date")
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

    def test_far_future_reset_is_ignored(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            {"123": {"ports": {"1": "Charging"}, "port_sessions": {"1": {"started_at": old_start}}}},
            stations,
            {"123-1": {"at_ms": ms_from_now(timedelta(hours=1)), "at": "2099-01-01T00:00:00Z"}},
        )

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], old_start)

    def test_invalid_persisted_started_at_self_heals(self):
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            {"123": {"ports": {"1": "Charging"}, "port_sessions": {"1": {"started_at": "not-a-date"}}}},
            stations,
            {},
        )

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)

    def test_unbounded_extension_cannot_hide_overdue_session(self):
        started_at = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        stations = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started_at}},
            }
        }

        poller.enrich_policy_complete_since(
            {},
            stations,
            {"123-1": {"until_ms": ms_from_now(timedelta(days=365))}},
            120,
        )

        self.assertIn("policy_complete_since", stations["123"]["port_sessions"]["1"])

    def test_extension_cannot_shorten_base_deadline(self):
        started_at = (datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat()
        stations = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started_at}},
            }
        }

        poller.enrich_policy_complete_since(
            {},
            stations,
            {"123-1": {"until_ms": ms_from_now(timedelta(minutes=-1))}},
            120,
        )

        self.assertNotIn("policy_complete_since", stations["123"]["port_sessions"]["1"])

    def test_fetch_error_preserves_previous_station_state_and_timer(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        prev_root = {
            "123": {
                "device_id": 123,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"123": {"error": "timeout", "updated_at": "2026-01-01T00:00:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["123"])
        self.assertEqual(stations["123"]["ports"], {"1": "Charging"})
        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], old_start)
        self.assertEqual(stations["123"]["last_fetch_error"], "timeout")


if __name__ == "__main__":
    unittest.main()
