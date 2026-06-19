import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

requests = types.ModuleType("requests")
requests.codes = types.SimpleNamespace(ok=200)
sys.modules.setdefault("requests", requests)

python_chargepoint = types.ModuleType("python_chargepoint")
python_chargepoint.ChargePoint = object
chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


chargepoint_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
chargepoint_exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", python_chargepoint)
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

import poller


def iso_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class PollerTimerMetadataTests(unittest.TestCase):
    def test_reset_uses_bounded_at_ms_not_untrusted_iso(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        old_ms = now_ms - 30 * poller.MILLISECONDS_PER_MINUTE
        reset_ms = now_ms - 5 * poller.MILLISECONDS_PER_MINUTE
        prev = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_from_ms(old_ms)}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}
        resets = {"1-1": {"at_ms": reset_ms, "at": "not-a-date"}}

        poller.enrich_stations_with_port_sessions(prev, stations, resets)
        started = stations["1"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started, "not-a-date")
        self.assertEqual(poller._iso_to_utc_ms(started), reset_ms)
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

    def test_future_reset_is_ignored(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        old_ms = now_ms - 30 * poller.MILLISECONDS_PER_MINUTE
        old_iso = iso_from_ms(old_ms)
        prev = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_iso}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}
        resets = {"1-1": {"at_ms": now_ms + 60 * poller.MILLISECONDS_PER_MINUTE}}

        poller.enrich_stations_with_port_sessions(prev, stations, resets)

        self.assertEqual(stations["1"]["port_sessions"]["1"]["started_at"], old_iso)

    def test_invalid_persisted_start_self_heals_without_crashing_policy(self):
        prev = {
            "1": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"1": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev, stations, {})
        started = stations["1"]["port_sessions"]["1"]["started_at"]
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

        self.assertNotEqual(started, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started), 0)

    def test_extension_deadline_cannot_shorten_or_extend_implausibly_far(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = now_ms - 60 * poller.MILLISECONDS_PER_MINUTE
        start_iso = iso_from_ms(start_ms)
        base = start_ms + 120 * poller.MILLISECONDS_PER_MINUTE
        valid = base + 15 * poller.MILLISECONDS_PER_MINUTE
        too_far = base + poller.MAX_EXTENSION_MS + 1

        self.assertEqual(
            poller._policy_deadline_ms(
                "1", "1", start_iso, 120, {"1-1": {"until_ms": base - 1}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "1", "1", start_iso, 120, {"1-1": {"until_ms": too_far}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "1", "1", start_iso, 120, {"1-1": {"until_ms": valid}}
            ),
            valid,
        )

    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        started = iso_from_ms(int(datetime.now(timezone.utc).timestamp() * 1000))
        prev = {
            "last_updated": started,
            "1": {
                "device_id": 1,
                "name": ["Station 1"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started}},
            },
        }
        stations = {"1": {"error": "boom", "updated_at": started}}

        poller.preserve_station_state_on_fetch_errors(prev, stations)

        self.assertNotIn("error", stations["1"])
        self.assertTrue(stations["1"]["stale"])
        self.assertEqual(stations["1"]["ports"], {"1": "Charging"})
        self.assertEqual(stations["1"]["port_sessions"]["1"]["started_at"], started)
        self.assertEqual(stations["1"]["last_fetch_error"], "boom")

    def test_rtdb_list_map_preserves_array_indices(self):
        self.assertEqual(poller._rtdb_map([None, "available", "charging"]), {
            "1": "available",
            "2": "charging",
        })


if __name__ == "__main__":
    unittest.main()
