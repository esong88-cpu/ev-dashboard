import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.initialize_app = lambda *args, **kwargs: None
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


def ms(iso):
    return poller._iso_to_utc_ms(iso)


def iso_from_ms(value):
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


class PollerTimerHardeningTest(unittest.TestCase):
    def test_reset_at_string_cannot_poison_started_at(self):
        now_ms = poller._utc_now_ms()
        old_start = iso_from_ms(now_ms - 2 * 60 * 60 * 1000)
        reset_ms = now_ms - 60 * 60 * 1000
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"100-1": {"at_ms": reset_ms, "at": "not a timestamp"}},
        )
        started_at = stations["100"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not a timestamp")
        self.assertEqual(ms(started_at), reset_ms)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        old_start = "2026-06-22T09:00:00+00:00"
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}
        far_future_ms = poller._utc_now_ms() + 24 * 60 * 60 * 1000

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"100-1": {"at_ms": far_future_ms, "at": "2099-01-01T00:00:00Z"}},
        )

        self.assertEqual(stations["100"]["port_sessions"]["1"]["started_at"], old_start)

    def test_invalid_previous_started_at_self_heals_without_crashing(self):
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not a timestamp"}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        started_at = stations["100"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not a timestamp")
        self.assertGreater(ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_public_extension_cannot_shorten_or_hide_deadline(self):
        start = "2026-06-22T10:00:00+00:00"
        base = ms(start) + 120 * 60 * 1000
        valid = base + 60 * 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", start, 120, {"100-1": {"until_ms": base - 1}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "100",
                "1",
                start,
                120,
                {"100-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 1}},
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", start, 120, {"100-1": {"until_ms": valid}}
            ),
            valid,
        )

    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        prev_root = {
            "100": {
                "name": ["Station 100"],
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-06-22T10:00:00+00:00"}
                },
            }
        }
        stations = {
            "100": {
                "error": "temporary ChargePoint failure",
                "updated_at": "2026-06-22T10:05:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["100"])
        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            "2026-06-22T10:00:00+00:00",
        )
        self.assertEqual(
            stations["100"]["last_fetch_error"], "temporary ChargePoint failure"
        )

    def test_empty_port_payload_preserves_previous_ports_and_sessions(self):
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-06-22T10:00:00+00:00"}
                },
            }
        }
        stations = {"100": {"ports": {}, "updated_at": "2026-06-22T10:05:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            "2026-06-22T10:00:00+00:00",
        )
        self.assertEqual(
            stations["100"]["last_fetch_error"], "station fetch returned no port data"
        )


if __name__ == "__main__":
    unittest.main()
