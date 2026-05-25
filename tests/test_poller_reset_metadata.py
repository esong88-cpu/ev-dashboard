import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda info: info)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)
    sys.modules.setdefault("requests", requests)

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = object
    sys.modules.setdefault("python_chargepoint", chargepoint)

    exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions.ChargePointLoginError = ChargePointLoginError
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_dependency_stubs()

import poller  # noqa: E402


class ResetMetadataTests(unittest.TestCase):
    def test_malformed_reset_iso_uses_numeric_timestamp(self):
        reset_dt = datetime(2026, 5, 25, 9, 30, tzinfo=timezone.utc)
        reset_ms = int(reset_dt.timestamp() * 1000)
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-05-25T09:00:00+00:00"}
                },
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at_ms": reset_ms, "at": "not-a-date"}},
        )

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            reset_dt.isoformat(),
        )
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_future_reset_timestamp_is_ignored(self):
        started_at = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        future_ms = int(
            (datetime.now(timezone.utc) + timedelta(days=365)).timestamp() * 1000
        )
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started_at}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at_ms": future_ms, "at": "2100-01-01T00:00:00Z"}},
        )

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            started_at,
        )


if __name__ == "__main__":
    unittest.main()
