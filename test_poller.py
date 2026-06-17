import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.initialize_app = lambda *args, **kwargs: None
credentials = types.ModuleType("firebase_admin.credentials")
credentials.Certificate = lambda *args, **kwargs: object()
db = types.ModuleType("firebase_admin.db")
db.reference = lambda *args, **kwargs: None
firebase_admin.credentials = credentials
firebase_admin.db = db
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", credentials)
sys.modules.setdefault("firebase_admin.db", db)

chargepoint = types.ModuleType("python_chargepoint")
chargepoint.ChargePoint = object
chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")
chargepoint_exceptions.ChargePointCommunicationException = Exception
chargepoint_exceptions.ChargePointLoginError = Exception
sys.modules.setdefault("python_chargepoint", chargepoint)
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

import poller


class PollerTimerHardeningTest(unittest.TestCase):
    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        started = "2026-06-17T08:00:00+00:00"
        prev_root = {
            "last_updated": "2026-06-17T09:00:00+00:00",
            "101": {
                "device_id": 101,
                "name": ["Station 101"],
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": started,
                        "policy_complete_since": "2026-06-17T10:00:00+00:00",
                    }
                },
            },
        }
        stations = {
            "101": {
                "error": "timeout",
                "updated_at": "2026-06-17T11:00:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)
        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual([], cleared)
        self.assertNotIn("error", stations["101"])
        self.assertEqual({"1": "Charging"}, stations["101"]["ports"])
        self.assertEqual(started, stations["101"]["port_sessions"]["1"]["started_at"])
        self.assertEqual("timeout", stations["101"]["last_fetch_error"])

    def test_empty_ports_preserve_previous_ports_and_sessions(self):
        started = "2026-06-17T08:00:00+00:00"
        prev_root = {
            "202": {
                "device_id": 202,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started}},
            }
        }
        stations = {
            "202": {
                "device_id": 202,
                "ports": {},
                "updated_at": "2026-06-17T11:00:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)
        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual({"1": "Charging"}, stations["202"]["ports"])
        self.assertEqual(started, stations["202"]["port_sessions"]["1"]["started_at"])
        self.assertEqual("Station fetch returned no ports", stations["202"]["last_fetch_error"])

    def test_far_future_reset_and_untrusted_iso_are_ignored(self):
        old_start = datetime.now(timezone.utc) - timedelta(hours=1)
        old_start_iso = old_start.isoformat()
        stations = {"101": {"ports": {"1": "Charging"}}}
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start_iso}},
            }
        }
        reset_map = {
            "101-1": {
                "at": "not-a-date",
                "at_ms": int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp() * 1000),
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        self.assertEqual(old_start_iso, stations["101"]["port_sessions"]["1"]["started_at"])

    def test_valid_reset_uses_numeric_timestamp_not_untrusted_iso(self):
        old_start = datetime.now(timezone.utc) - timedelta(hours=1)
        reset = datetime.now(timezone.utc) - timedelta(minutes=5)
        stations = {"101": {"ports": {"1": "Charging"}}}
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        reset_map = {"101-1": {"at": "not-a-date", "at_ms": int(reset.timestamp() * 1000)}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)
        started_at = stations["101"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual("not-a-date", started_at)
        self.assertAlmostEqual(
            int(reset.timestamp() * 1000),
            poller._iso_to_utc_ms(started_at),
            delta=1000,
        )

    def test_invalid_persisted_started_at_does_not_crash_policy_enrichment(self):
        stations = {"101": {"ports": {"1": "Charging"}}}
        prev_root = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        started_at = stations["101"]["port_sessions"]["1"]["started_at"]
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)

    def test_extension_deadline_cannot_shorten_or_extend_past_bound(self):
        now = int(datetime(2026, 6, 17, 12, tzinfo=timezone.utc).timestamp() * 1000)
        started = datetime(2026, 6, 17, 11, tzinfo=timezone.utc).isoformat()
        base = poller._iso_to_utc_ms(started) + 120 * 60 * 1000
        valid_until = base + 15 * 60 * 1000

        self.assertEqual(
            base,
            poller._policy_deadline_ms(
                "101", "1", started, 120, {"101-1": {"until_ms": base - 1}}, now
            ),
        )
        self.assertEqual(
            base,
            poller._policy_deadline_ms(
                "101",
                "1",
                started,
                120,
                {"101-1": {"until_ms": now + poller.MAX_EXTENSION_MS + 1}},
                now,
            ),
        )
        self.assertEqual(
            valid_until,
            poller._policy_deadline_ms(
                "101", "1", started, 120, {"101-1": {"until_ms": valid_until}}, now
            ),
        )


if __name__ == "__main__":
    unittest.main()
