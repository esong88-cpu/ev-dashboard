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

chargepoint_mod = types.ModuleType("python_chargepoint")
chargepoint_mod.ChargePoint = object
chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


chargepoint_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
chargepoint_exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", chargepoint_mod)
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

import poller


class PollerTimerStateTests(unittest.TestCase):
    def test_rtdb_map_preserves_array_indices_as_keys(self):
        self.assertEqual(poller._rtdb_map([None, "Available", "Charging"]), {
            "1": "Available",
            "2": "Charging",
        })

    def test_reset_uses_bounded_numeric_timestamp_not_untrusted_string(self):
        previous_start = datetime.now(timezone.utc) - timedelta(hours=2)
        reset_ms = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1000)
        stations = {"123": {"ports": {"1": "Charging"}}}
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": previous_start.isoformat()}},
            }
        }

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "not-a-date", "at_ms": reset_ms}},
        )

        started = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(poller._iso_to_utc_ms(started), reset_ms)
        self.assertNotEqual(started, "not-a-date")

    def test_far_future_reset_timestamp_is_ignored(self):
        previous_start = datetime.now(timezone.utc) - timedelta(hours=2)
        far_future_ms = int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp() * 1000)
        stations = {"123": {"ports": {"1": "Charging"}}}
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": previous_start.isoformat()}},
            }
        }

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "2099-01-01T00:00:00Z", "at_ms": far_future_ms}},
        )

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            previous_start.isoformat(),
        )

    def test_invalid_previous_start_self_heals_without_crashing(self):
        stations = {"123": {"ports": {"1": "Charging"}}}
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        started = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertGreater(poller._iso_to_utc_ms(started), 0)

    def test_extension_deadline_cannot_shorten_or_extend_implausibly_far(self):
        started_at = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        start_ms = poller._iso_to_utc_ms(started_at)
        base = start_ms + 120 * 60 * 1000
        valid_until = base + 30 * 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms(
                "123", "1", started_at, 120, {"123-1": {"until_ms": base - 1}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 1}},
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "123", "1", started_at, 120, {"123-1": {"until_ms": valid_until}}
            ),
            valid_until,
        )

    def test_fetch_error_preserves_previous_ports_and_sessions_before_replace(self):
        prev_root = {
            "123": {
                "name": ["Station A"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
                "updated_at": "2026-01-01T00:00:00+00:00",
            }
        }
        stations = {
            "123": {
                "error": "temporary outage",
                "updated_at": "2026-01-01T00:05:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["123"])
        self.assertEqual(stations["123"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["123"]["port_sessions"],
            {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
        )
        self.assertTrue(stations["123"]["stale"])
        self.assertEqual(stations["123"]["last_fetch_error"], "temporary outage")

    def test_available_port_retries_cleanup_for_orphaned_metadata(self):
        prev_root = {"123": {"ports": {"1": "Available"}}}
        stations = {"123": {"ports": {"1": "Available"}}}

        cleared = poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at_ms": 123}},
            {"123-1": {"until_ms": 456}},
        )

        self.assertEqual(cleared, ["123-1"])


if __name__ == "__main__":
    unittest.main()
