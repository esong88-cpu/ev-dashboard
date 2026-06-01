import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)
    sys.modules.setdefault("requests", requests)

    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda *args, **kwargs: object())
    firebase_admin.db = types.SimpleNamespace(reference=lambda *args, **kwargs: None)
    sys.modules.setdefault("firebase_admin", firebase_admin)

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = object
    sys.modules.setdefault("python_chargepoint", chargepoint)

    exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        def __init__(self, response=None, message=""):
            super().__init__(message)
            self.response = response

    class ChargePointLoginError(Exception):
        pass

    exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions.ChargePointLoginError = ChargePointLoginError
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_dependency_stubs()

import poller  # noqa: E402


class PublicMetadataValidationTests(unittest.TestCase):
    def test_huge_future_reset_is_ignored_without_crashing(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {"123-1": {"at_ms": 10**30}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], old_start)

    def test_reset_uses_bounded_numeric_timestamp_not_public_iso_string(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        reset_ms = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {
            "123-1": {
                "at": "2999-01-01T00:00:00+00:00",
                "at_ms": reset_ms,
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

    def test_corrupt_future_started_at_is_repaired(self):
        old_start = (datetime.now(timezone.utc) + timedelta(days=3650)).isoformat()
        before_ms = poller._now_utc_ms()
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        started_ms = poller._iso_to_utc_ms(started_at)
        self.assertNotEqual(started_at, old_start)
        self.assertGreaterEqual(started_ms, before_ms)
        self.assertLessEqual(started_ms, poller._now_utc_ms() + poller.RESET_CLOCK_SKEW_MS)

    def test_public_extension_cannot_shorten_or_indefinitely_extend_deadline(self):
        now_ms = poller._now_utc_ms()
        start = datetime.fromtimestamp((now_ms - 3 * 60 * 60 * 1000) / 1000, tz=timezone.utc).isoformat()
        base = poller._iso_to_utc_ms(start) + 120 * 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms("123", "1", start, 120, {"123-1": {"until_ms": 1}}, now_ms),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                start,
                120,
                {"123-1": {"until_ms": now_ms + poller.MAX_EXTENSION_FUTURE_MS + 1}},
                now_ms,
            ),
            base,
        )

        valid_until = now_ms + 60 * 60 * 1000
        self.assertEqual(
            poller._policy_deadline_ms(
                "123", "1", start, 120, {"123-1": {"until_ms": valid_until}}, now_ms
            ),
            valid_until,
        )

    def test_invalid_iso_timestamp_is_not_fatal(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)


if __name__ == "__main__":
    unittest.main()
