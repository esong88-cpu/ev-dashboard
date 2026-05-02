import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def load_poller():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
    firebase_admin.db = types.SimpleNamespace()
    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)
    sys.modules.setdefault("requests", requests)

    chargepoint_mod = types.ModuleType("python_chargepoint")
    chargepoint_mod.ChargePoint = object
    exceptions_mod = types.ModuleType("python_chargepoint.exceptions")
    exceptions_mod.ChargePointCommunicationException = type(
        "ChargePointCommunicationException", (Exception,), {}
    )
    exceptions_mod.ChargePointLoginError = type("ChargePointLoginError", (Exception,), {})
    sys.modules.setdefault("python_chargepoint", chargepoint_mod)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions_mod)

    return importlib.import_module("poller")


class PollerMetadataTest(unittest.TestCase):
    def setUp(self):
        self.poller = load_poller()

    def test_policy_deadline_ignores_implausibly_future_extension(self):
        started = datetime.now(timezone.utc) - timedelta(hours=3)
        started_iso = started.isoformat()
        far_future = int(
            (
                datetime.now(timezone.utc)
                + timedelta(milliseconds=self.poller.METADATA_MAX_FUTURE_MS + 60_000)
            ).timestamp()
            * 1000
        )

        deadline = self.poller._policy_deadline_ms(
            "station", "1", started_iso, 120, {"station-1": {"until_ms": far_future}}
        )

        self.assertEqual(deadline, int(started.timestamp() * 1000) + 120 * 60 * 1000)

    def test_policy_deadline_never_shortens_base_deadline(self):
        started = datetime.now(timezone.utc).isoformat()
        deadline = self.poller._policy_deadline_ms(
            "station", "1", started, 120, {"station-1": {"until_ms": 1}}
        )

        self.assertGreater(deadline, 1)

    def test_future_reset_does_not_move_session_start(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        far_future_reset_ms = int(
            (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp() * 1000
        )
        prev_root = {
            "station": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"station": {"ports": {"1": "Charging"}}}

        self.poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"station-1": {"at_ms": far_future_reset_ms}},
        )

        self.assertEqual(stations["station"]["port_sessions"]["1"]["started_at"], old_start)


if __name__ == "__main__":
    unittest.main()
