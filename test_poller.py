import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_import_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    sys.modules.setdefault("firebase_admin", firebase_admin)

    chargepoint_mod = types.ModuleType("python_chargepoint")

    class ChargePoint:
        pass

    chargepoint_mod.ChargePoint = ChargePoint
    sys.modules.setdefault("python_chargepoint", chargepoint_mod)

    exceptions_mod = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions_mod.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions_mod.ChargePointLoginError = ChargePointLoginError
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions_mod)


_install_import_stubs()

import poller  # noqa: E402


def _iso_delta(**kwargs):
    return (datetime.now(timezone.utc) + timedelta(**kwargs)).isoformat()


def _ms_delta(**kwargs):
    return int((datetime.now(timezone.utc) + timedelta(**kwargs)).timestamp() * 1000)


class PublicMetadataTrustBoundaryTest(unittest.TestCase):
    def test_malformed_reset_display_string_does_not_poison_session_start(self):
        old_start = _iso_delta(hours=-3)
        reset_ms = _ms_delta()
        stations = {"101": {"ports": {"1": "Charging"}}}
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"101-1": {"at": "not-a-date", "at_ms": reset_ms}},
        )
        started_at = stations["101"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), poller._iso_to_utc_ms(old_start))
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored_and_overdue_session_remains_complete(self):
        old_start = _iso_delta(hours=-3)
        stations = {"101": {"ports": {"1": "Charging"}}}
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"101-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": _ms_delta(days=30)}},
        )
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        session = stations["101"]["port_sessions"]["1"]
        self.assertEqual(session["started_at"], old_start)
        self.assertIn("policy_complete_since", session)

    def test_far_future_extension_cannot_hide_overdue_session(self):
        old_start = _iso_delta(hours=-3)
        stations = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        prev_root = {"101": {"port_sessions": {"1": {"started_at": old_start}}}}

        poller.enrich_policy_complete_since(
            prev_root,
            stations,
            {"101-1": {"until_ms": _ms_delta(days=30)}},
            120,
        )

        self.assertIn("policy_complete_since", stations["101"]["port_sessions"]["1"])

    def test_reasonable_extension_can_defer_policy_complete(self):
        old_start = _iso_delta(hours=-3)
        stations = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        prev_root = {"101": {"port_sessions": {"1": {"started_at": old_start}}}}

        poller.enrich_policy_complete_since(
            prev_root,
            stations,
            {"101-1": {"until_ms": _ms_delta(minutes=30)}},
            120,
        )

        self.assertNotIn("policy_complete_since", stations["101"]["port_sessions"]["1"])

    def test_invalid_iso_fails_closed(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)


if __name__ == "__main__":
    unittest.main()
