import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda info: info)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

python_chargepoint = types.ModuleType("python_chargepoint")
python_chargepoint.ChargePoint = object
python_chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


python_chargepoint_exceptions.ChargePointCommunicationException = (
    ChargePointCommunicationException
)
python_chargepoint_exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", python_chargepoint)
sys.modules.setdefault("python_chargepoint.exceptions", python_chargepoint_exceptions)

import poller


class ResetSessionTests(unittest.TestCase):
    def test_reset_uses_at_ms_not_untrusted_at_string(self):
        previous_start = datetime.now(timezone.utc) - timedelta(minutes=30)
        reset_ms = poller._iso_to_utc_ms(previous_start.isoformat()) + 60_000
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": previous_start.isoformat()}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"101-1": {"at": "not-a-date", "at_ms": reset_ms}},
        )

        self.assertEqual(
            stations["101"]["port_sessions"]["1"]["started_at"],
            datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat(),
        )

    def test_future_reset_is_ignored_and_policy_still_marks_complete(self):
        previous_start = datetime.now(timezone.utc) - timedelta(hours=3)
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": previous_start.isoformat()}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}
        future_reset_ms = int(
            (datetime.now(timezone.utc) + timedelta(days=1)).timestamp() * 1000
        )

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"101-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": future_reset_ms}},
        )
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        session = stations["101"]["port_sessions"]["1"]
        self.assertEqual(session["started_at"], previous_start.isoformat())
        self.assertIn("policy_complete_since", session)

    def test_invalid_previous_started_at_self_heals_without_crashing(self):
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        started_at = stations["101"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)


if __name__ == "__main__":
    unittest.main()
