import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_import_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda info: info)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    sys.modules["firebase_admin"] = firebase_admin
    sys.modules["firebase_admin.credentials"] = firebase_admin.credentials
    sys.modules["firebase_admin.db"] = firebase_admin.db

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)
    sys.modules["requests"] = requests

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


_install_import_stubs()
poller = importlib.import_module("poller")


class ResetValidationTests(unittest.TestCase):
    def test_malformed_future_reset_does_not_poison_started_at_or_crash_policy(self):
        old_start = "2026-01-01T00:00:00+00:00"
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}
        reset_map = {
            "1-1": {
                "at": "not-an-iso-date",
                "at_ms": 4102444800000,
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(stations["1"]["port_sessions"]["1"]["started_at"], old_start)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_valid_reset_uses_numeric_at_ms_not_public_at_string(self):
        now = datetime.now(timezone.utc)
        old_start = now - timedelta(hours=2)
        reset_at = now - timedelta(minutes=1)
        reset_ms = int(reset_at.timestamp() * 1000)
        expected_start = datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat()
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}
        reset_map = {
            "1-1": {
                "at": "not-an-iso-date",
                "at_ms": reset_ms,
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(stations["1"]["port_sessions"]["1"]["started_at"], expected_start)


if __name__ == "__main__":
    unittest.main()
