import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

chargepoint_module = types.ModuleType("python_chargepoint")
chargepoint_module.ChargePoint = object
chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")
chargepoint_exceptions.ChargePointCommunicationException = type(
    "ChargePointCommunicationException", (Exception,), {}
)
chargepoint_exceptions.ChargePointLoginError = type("ChargePointLoginError", (Exception,), {})
sys.modules.setdefault("python_chargepoint", chargepoint_module)
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

requests_module = types.ModuleType("requests")
requests_module.codes = types.SimpleNamespace(ok=200)
sys.modules.setdefault("requests", requests_module)

import poller


class PollerSessionMetadataTests(unittest.TestCase):
    def test_malformed_reset_iso_uses_valid_reset_ms(self):
        now = datetime.now(timezone.utc)
        old_start = now - timedelta(hours=1)
        reset = now - timedelta(seconds=30)
        reset_ms = int(reset.timestamp() * 1000)
        stations = {"123": {"ports": {"1": "Charging"}}}
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "not-an-iso-date", "at_ms": reset_ms}},
        )
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

    def test_future_reset_is_ignored(self):
        now = datetime.now(timezone.utc)
        old_start = now - timedelta(hours=1)
        future_reset = now + timedelta(milliseconds=poller.RESET_FUTURE_SKEW_MS + 60000)
        stations = {"123": {"ports": {"1": "Charging"}}}
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": future_reset.isoformat(), "at_ms": int(future_reset.timestamp() * 1000)}},
        )

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], old_start.isoformat())

    def test_bad_existing_session_timestamp_does_not_crash_policy_enrichment(self):
        stations = {"123": {"ports": {"1": "Charging"}, "port_sessions": {"1": {"started_at": "bad"}}}}

        poller.enrich_policy_complete_since({}, stations, {}, 120)

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], "bad")
        self.assertNotIn("policy_complete_since", stations["123"]["port_sessions"]["1"])


if __name__ == "__main__":
    unittest.main()
