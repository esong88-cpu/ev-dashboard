import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

    chargepoint_mod = types.ModuleType("python_chargepoint")

    class ChargePoint:
        pass

    chargepoint_mod.ChargePoint = ChargePoint
    exceptions_mod = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions_mod.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions_mod.ChargePointLoginError = ChargePointLoginError
    sys.modules.setdefault("python_chargepoint", chargepoint_mod)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions_mod)


_install_dependency_stubs()

import poller


class SlotResetValidationTest(unittest.TestCase):
    def _occupied_station(self):
        return {"123": {"device_id": 123, "ports": {"1": "Charging"}}}

    def test_malformed_reset_display_string_does_not_crash_policy_enrichment(self):
        now = datetime.now(timezone.utc)
        old_start = (now - timedelta(hours=3)).isoformat()
        reset_ms = int((now - timedelta(minutes=5)).timestamp() * 1000)
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = self._occupied_station()

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "not-a-date", "at_ms": reset_ms}},
        )
        started_at = stations["123"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-date")
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        now = datetime.now(timezone.utc)
        old_start = (now - timedelta(hours=3)).isoformat()
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = self._occupied_station()
        future_ms = int((now + timedelta(hours=1)).timestamp() * 1000)

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": future_ms}},
        )

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], old_start)

    def test_invalid_previous_session_start_is_healed(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = self._occupied_station()

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        started_at = stations["123"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)


if __name__ == "__main__":
    unittest.main()
