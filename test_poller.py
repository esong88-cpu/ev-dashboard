import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.initialize_app = lambda *args, **kwargs: None
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

chargepoint_module = types.ModuleType("python_chargepoint")
chargepoint_module.ChargePoint = type("ChargePoint", (), {})
sys.modules.setdefault("python_chargepoint", chargepoint_module)

chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")
chargepoint_exceptions.ChargePointCommunicationException = type(
    "ChargePointCommunicationException", (Exception,), {}
)
chargepoint_exceptions.ChargePointLoginError = type(
    "ChargePointLoginError", (Exception,), {}
)
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

import poller


class PollerResetTimestampTests(unittest.TestCase):
    def test_iso_parse_failure_returns_zero_instead_of_crashing(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)

    def test_malformed_reset_iso_uses_numeric_timestamp(self):
        reset_dt = datetime.now(timezone.utc) - timedelta(minutes=5)
        reset_ms = int(reset_dt.timestamp() * 1000)
        prev = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": (
                            datetime.now(timezone.utc) - timedelta(hours=1)
                        ).isoformat()
                    }
                },
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev, stations, {"123-1": {"at_ms": reset_ms, "at": "not-a-date"}}
        )
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

        started = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(poller._iso_to_utc_ms(started), reset_ms)

    def test_future_reset_timestamp_is_clamped_to_poller_clock(self):
        before_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        future_ms = int(
            (datetime.now(timezone.utc) + timedelta(days=365)).timestamp() * 1000
        )
        prev = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": (
                            datetime.now(timezone.utc) - timedelta(hours=1)
                        ).isoformat()
                    }
                },
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev, stations, {"123-1": {"at_ms": future_ms, "at": "not-a-date"}}
        )

        started_ms = poller._iso_to_utc_ms(
            stations["123"]["port_sessions"]["1"]["started_at"]
        )
        after_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.assertGreaterEqual(started_ms, before_ms)
        self.assertLessEqual(started_ms, after_ms)

    def test_invalid_previous_started_at_is_replaced(self):
        before_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        prev = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev, stations, {})

        started_ms = poller._iso_to_utc_ms(
            stations["123"]["port_sessions"]["1"]["started_at"]
        )
        after_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.assertGreaterEqual(started_ms, before_ms)
        self.assertLessEqual(started_ms, after_ms)


if __name__ == "__main__":
    unittest.main()
