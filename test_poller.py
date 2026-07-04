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
exceptions_module = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions_module.ChargePointCommunicationException = ChargePointCommunicationException
exceptions_module.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", chargepoint_module)
sys.modules.setdefault("python_chargepoint.exceptions", exceptions_module)

import poller


def iso_for_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class PollerTimerHardeningTest(unittest.TestCase):
    def test_reset_uses_bounded_numeric_timestamp_not_untrusted_iso(self):
        start_ms = poller._utc_now_ms() - 30 * 60 * 1000
        reset_ms = start_ms + 5 * 60 * 1000
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_for_ms(start_ms)}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        resets = {"123-1": {"at_ms": reset_ms, "at": "not an iso timestamp"}}

        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, resets)

        self.assertEqual([], cleared)
        self.assertEqual(iso_for_ms(reset_ms), stations["123"]["port_sessions"]["1"]["started_at"])
        poller.enrich_policy_complete_since(prev_root, stations, {}, 1)

    def test_far_future_reset_is_ignored(self):
        start_ms = poller._utc_now_ms() - 30 * 60 * 1000
        reset_ms = poller._utc_now_ms() + 24 * 60 * 60 * 1000
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_for_ms(start_ms)}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        resets = {"123-1": {"at_ms": reset_ms, "at": iso_for_ms(reset_ms)}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, resets)

        self.assertEqual(iso_for_ms(start_ms), stations["123"]["port_sessions"]["1"]["started_at"])

    def test_invalid_persisted_session_start_self_heals(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not an iso timestamp"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        started = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual("not an iso timestamp", started)
        self.assertGreater(poller._iso_to_utc_ms(started), 0)

    def test_public_extension_deadline_cannot_shorten_or_exceed_cap(self):
        start = datetime.now(timezone.utc) - timedelta(hours=3)
        start_ms = int(start.timestamp() * 1000)
        base = start_ms + 120 * 60 * 1000

        shortened = poller._policy_deadline_ms(
            "123",
            "1",
            start.isoformat(),
            120,
            {"123-1": {"until_ms": base - 60 * 1000}},
        )
        too_long = poller._policy_deadline_ms(
            "123",
            "1",
            start.isoformat(),
            120,
            {"123-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 1}},
        )
        valid = poller._policy_deadline_ms(
            "123",
            "1",
            start.isoformat(),
            120,
            {"123-1": {"until_ms": base + 60 * 60 * 1000}},
        )

        self.assertEqual(base, shortened)
        self.assertEqual(base, too_long)
        self.assertEqual(base + 60 * 60 * 1000, valid)

    def test_fetch_error_preserves_previous_station_and_session_state(self):
        prev_root = {
            "123": {
                "device_id": 123,
                "name": ["Station 123"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
            }
        }
        stations = {"123": {"error": "temporary API failure", "updated_at": "2026-01-01T00:05:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["123"])
        self.assertEqual({"1": "Charging"}, stations["123"]["ports"])
        self.assertEqual(prev_root["123"]["port_sessions"], stations["123"]["port_sessions"])
        self.assertEqual("temporary API failure", stations["123"]["last_fetch_error"])

    def test_available_port_retries_stale_metadata_cleanup(self):
        prev_root = {"123": {"ports": {"1": "Available"}}}
        stations = {"123": {"ports": {"1": "Available"}}}

        cleared = poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at_ms": poller._utc_now_ms()}},
            {"123-1": {"until_ms": poller._utc_now_ms() + 60 * 1000}},
        )

        self.assertEqual(["123-1"], cleared)


if __name__ == "__main__":
    unittest.main()
