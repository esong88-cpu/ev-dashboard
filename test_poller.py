import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda info: info)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    firebase_admin.initialize_app = lambda cred, options: None

    chargepoint_mod = types.ModuleType("python_chargepoint")
    chargepoint_mod.ChargePoint = object
    exceptions_mod = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions_mod.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions_mod.ChargePointLoginError = ChargePointLoginError

    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)
    sys.modules.setdefault("python_chargepoint", chargepoint_mod)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions_mod)


_install_dependency_stubs()
poller = importlib.import_module("poller")


def iso_for(delta: timedelta) -> str:
    return (datetime.now(timezone.utc) + delta).isoformat()


def ms_for(delta: timedelta) -> int:
    return int((datetime.now(timezone.utc) + delta).timestamp() * 1000)


class PollerTimerHardeningTests(unittest.TestCase):
    def test_reset_uses_bounded_numeric_timestamp_not_untrusted_iso(self):
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_for(timedelta(hours=-1))}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}
        reset_ms = ms_for(timedelta(seconds=-5))
        reset_map = {"101-1": {"at_ms": reset_ms, "at": "not-a-date"}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)
        started_at = stations["101"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-date")
        self.assertAlmostEqual(poller._iso_to_utc_ms(started_at), reset_ms, delta=1000)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        original_start = iso_for(timedelta(hours=-1))
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": original_start}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}
        reset_map = {"101-1": {"at_ms": ms_for(timedelta(days=1)), "at": "2035-01-01T00:00:00Z"}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(stations["101"]["port_sessions"]["1"]["started_at"], original_start)

    def test_invalid_persisted_start_self_heals_without_crashing_policy(self):
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        started_at = stations["101"]["port_sessions"]["1"]["started_at"]

        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_extension_deadline_cannot_shorten_or_extend_beyond_cap(self):
        started_at = iso_for(timedelta(hours=-3))
        base = poller._iso_to_utc_ms(started_at) + 120 * 60 * 1000

        shortened = poller._policy_deadline_ms(
            "101", "1", started_at, 120, {"101-1": {"until_ms": 1}}
        )
        far_future = poller._policy_deadline_ms(
            "101",
            "1",
            started_at,
            120,
            {"101-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 86_400_000}},
        )

        self.assertEqual(shortened, base)
        self.assertEqual(far_future, base + poller.MAX_EXTENSION_MS)

    def test_far_future_extension_still_marks_long_overdue_session_complete(self):
        prev_root = {}
        stations = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_for(timedelta(hours=-12))}},
            }
        }

        poller.enrich_policy_complete_since(
            prev_root,
            stations,
            {"101-1": {"until_ms": ms_for(timedelta(days=365))}},
            120,
        )

        self.assertIn("policy_complete_since", stations["101"]["port_sessions"]["1"])

    def test_fetch_error_preserves_previous_ports_and_session_clock(self):
        previous_start = iso_for(timedelta(hours=-4))
        prev_root = {
            "101": {
                "device_id": 101,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": previous_start}},
            }
        }
        stations = {"101": {"error": "timeout", "updated_at": iso_for(timedelta())}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["101"])
        self.assertEqual(stations["101"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"], previous_start
        )
        self.assertEqual(stations["101"]["last_fetch_error"], "timeout")

    def test_empty_port_payload_preserves_previous_ports_and_session_clock(self):
        previous_start = iso_for(timedelta(hours=-4))
        prev_root = {
            "101": {
                "device_id": 101,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": previous_start}},
            }
        }
        stations = {"101": {"device_id": 101, "ports": {}}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertEqual(stations["101"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"], previous_start
        )
        self.assertTrue(stations["101"]["stale"])


if __name__ == "__main__":
    unittest.main()
