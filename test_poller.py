import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.initialize_app = lambda *args, **kwargs: None
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda info: info)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin)

chargepoint = types.ModuleType("python_chargepoint")
chargepoint.ChargePoint = object
sys.modules.setdefault("python_chargepoint", chargepoint)

chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


chargepoint_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
chargepoint_exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint.exceptions", chargepoint_exceptions)

requests = types.ModuleType("requests")
requests.codes = types.SimpleNamespace(ok=200)
sys.modules.setdefault("requests", requests)

import poller


def utc_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


class PollerTimerHardeningTest(unittest.TestCase):
    def test_malformed_public_reset_at_does_not_poison_started_at(self):
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-07-01T09:00:00+00:00"}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}
        reset_ms = utc_ms("2026-07-01T10:00:00+00:00")

        poller.enrich_stations_with_port_sessions(
            prev,
            stations,
            {"100-1": {"at": "not an iso timestamp", "at_ms": reset_ms}},
        )

        started_at = stations["100"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(started_at, "2026-07-01T10:00:00+00:00")
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

    def test_invalid_previous_started_at_self_heals_before_policy_enrichment(self):
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "bad timestamp"}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev, stations, {})
        started_at = stations["100"]["port_sessions"]["1"]["started_at"]

        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

    def test_public_extension_deadline_cannot_shorten_or_exceed_cap(self):
        started_at = "2026-07-01T08:00:00+00:00"
        base = utc_ms(started_at) + 120 * 60 * 1000

        shortened = poller._policy_deadline_ms(
            "100", "1", started_at, 120, {"100-1": {"until_ms": base - 1}}
        )
        too_far = poller._policy_deadline_ms(
            "100",
            "1",
            started_at,
            120,
            {"100-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 1}},
        )

        self.assertEqual(shortened, base)
        self.assertEqual(too_far, base + poller.MAX_EXTENSION_MS)

    def test_future_reset_outside_clock_skew_is_ignored(self):
        now_ms = poller._utc_now_ms()
        started_at = poller._ms_to_utc_iso(now_ms - 60 * 60 * 1000)
        reset = {"at_ms": now_ms + poller.MAX_RESET_FUTURE_SKEW_MS + 1}

        self.assertEqual(poller._reset_started_at(reset, started_at, now_ms), started_at)

    def test_fetch_errors_preserve_previous_ports_and_sessions(self):
        prev = {
            "100": {
                "name": ["Station 100"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-07-01T09:00:00+00:00"}},
            }
        }
        stations = {"100": {"error": "upstream timeout", "updated_at": "2026-07-01T11:00:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev, stations)

        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            "2026-07-01T09:00:00+00:00",
        )
        self.assertEqual(stations["100"]["last_fetch_error"], "upstream timeout")

    def test_empty_port_payload_preserves_previous_ports_and_sessions(self):
        prev = {
            "100": {
                "ports": {"1": "Complete"},
                "port_sessions": {"1": {"started_at": "2026-07-01T07:00:00+00:00"}},
            }
        }
        stations = {"100": {"ports": {}, "updated_at": "2026-07-01T11:00:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev, stations)

        self.assertEqual(stations["100"]["ports"], {"1": "Complete"})
        self.assertIn("port_sessions", stations["100"])
        self.assertEqual(stations["100"]["last_fetch_error"], "Station returned no port data")


if __name__ == "__main__":
    unittest.main()
