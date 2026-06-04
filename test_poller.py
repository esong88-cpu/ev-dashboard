import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


if "firebase_admin" not in sys.modules:
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.initialize_app = lambda *args, **kwargs: None

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda *args, **kwargs: object()

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda *args, **kwargs: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    sys.modules["firebase_admin"] = firebase_admin
    sys.modules["firebase_admin.credentials"] = credentials
    sys.modules["firebase_admin.db"] = db

if "python_chargepoint" not in sys.modules:
    chargepoint = types.ModuleType("python_chargepoint")

    class ChargePoint:
        pass

    chargepoint.ChargePoint = ChargePoint
    exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions.ChargePointLoginError = ChargePointLoginError
    sys.modules["python_chargepoint"] = chargepoint
    sys.modules["python_chargepoint.exceptions"] = exceptions

import poller


class PollerMetadataHardeningTest(unittest.TestCase):
    def test_poisoned_reset_at_string_does_not_crash_or_persist(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        reset_ms = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}
        resets = {"1-1": {"at_ms": reset_ms, "at": "not-a-valid-iso"}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, resets)
        started_at = stations["1"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-valid-iso")
        self.assertEqual(started_at, datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat())
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        prev_root = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}
        resets = {
            "1-1": {
                "at_ms": int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp() * 1000),
                "at": "2099-01-01T00:00:00+00:00",
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, resets)

        self.assertEqual(stations["1"]["port_sessions"]["1"]["started_at"], old_start)

    def test_untrusted_extension_deadlines_are_bounded(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        started_at = datetime.fromtimestamp((now_ms - 60 * 60 * 1000) / 1000, tz=timezone.utc).isoformat()
        base = poller._iso_to_utc_ms(started_at) + 120 * 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms("1", "1", started_at, 120, {"1-1": {"until_ms": base - 1}}, now_ms),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms("1", "1", started_at, 120, {"1-1": {"until_ms": 9e15}}, now_ms),
            base,
        )
        valid_until = max(base, now_ms) + 60 * 60 * 1000
        self.assertEqual(
            poller._policy_deadline_ms("1", "1", started_at, 120, {"1-1": {"until_ms": valid_until}}, now_ms),
            valid_until,
        )

    def test_fetch_error_preserves_previous_port_session_state(self):
        prev_root = {
            "1": {
                "device_id": 1,
                "name": ["Station 1"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
                "source": "public",
            }
        }
        stations = {"1": {"error": "timeout", "updated_at": "2026-01-01T01:00:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertTrue(stations["1"]["stale"])
        self.assertEqual(stations["1"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["1"]["port_sessions"],
            {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
        )
        self.assertEqual(stations["1"]["error"], "timeout")


if __name__ == "__main__":
    unittest.main()
