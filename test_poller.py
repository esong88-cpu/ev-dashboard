import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.initialize_app = lambda *args, **kwargs: None
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    sys.modules["firebase_admin"] = firebase_admin
    sys.modules["firebase_admin.credentials"] = firebase_admin.credentials
    sys.modules["firebase_admin.db"] = firebase_admin.db

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = object
    exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions.ChargePointLoginError = ChargePointLoginError
    sys.modules["python_chargepoint"] = chargepoint
    sys.modules["python_chargepoint.exceptions"] = exceptions


_install_dependency_stubs()
poller = importlib.import_module("poller")


class PollerHardeningTest(unittest.TestCase):
    def test_reset_uses_bounded_numeric_timestamp_not_public_iso_string(self):
        now = datetime.now(timezone.utc)
        old_start = now - timedelta(hours=1)
        reset_ms = int((now - timedelta(minutes=30)).timestamp() * 1000)
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev,
            stations,
            {"100-1": {"at_ms": reset_ms, "at": "not a timestamp"}},
        )

        started = stations["100"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started, "not a timestamp")
        self.assertAlmostEqual(poller._iso_to_utc_ms(started), reset_ms, delta=1)
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

    def test_invalid_persisted_start_self_heals_without_policy_crash(self):
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not a timestamp"}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev, stations, {})
        started = stations["100"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started, "not a timestamp")
        self.assertGreater(poller._iso_to_utc_ms(started), 0)
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        now = datetime.now(timezone.utc)
        old_start = now - timedelta(hours=1)
        future_ms = int((now + timedelta(days=1)).timestamp() * 1000)
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev,
            stations,
            {"100-1": {"at_ms": future_ms, "at": datetime.fromtimestamp(future_ms / 1000, tz=timezone.utc).isoformat()}},
        )

        self.assertEqual(stations["100"]["port_sessions"]["1"]["started_at"], old_start.isoformat())

    def test_extension_deadline_cannot_shorten_or_exceed_cap(self):
        start = datetime.now(timezone.utc) - timedelta(hours=3)
        start_ms = int(start.timestamp() * 1000)
        base = start_ms + 120 * 60 * 1000

        shortened = poller._policy_deadline_ms(
            "100", "1", start.isoformat(), 120, {"100-1": {"until_ms": base - 60 * 60 * 1000}}
        )
        extended = poller._policy_deadline_ms(
            "100", "1", start.isoformat(), 120, {"100-1": {"until_ms": base + 72 * 60 * 60 * 1000}}
        )

        self.assertEqual(shortened, base)
        self.assertEqual(extended, base + poller.MAX_EXTENSION_MS)

    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        prev = {
            "100": {
                "name": ["Station 100"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
            }
        }
        stations = {"100": {"error": "timeout", "updated_at": "2026-01-01T01:00:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev, stations)

        self.assertNotIn("error", stations["100"])
        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"],
            {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
        )
        self.assertTrue(stations["100"]["stale"])
        self.assertEqual(stations["100"]["last_fetch_error"], "timeout")


if __name__ == "__main__":
    unittest.main()
