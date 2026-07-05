import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda *_args, **_kwargs: object())
firebase_admin.db = types.SimpleNamespace(reference=lambda *_args, **_kwargs: None)
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

chargepoint_mod = types.ModuleType("python_chargepoint")


class ChargePoint:
    pass


chargepoint_mod.ChargePoint = ChargePoint
exceptions_mod = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions_mod.ChargePointCommunicationException = ChargePointCommunicationException
exceptions_mod.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", chargepoint_mod)
sys.modules.setdefault("python_chargepoint.exceptions", exceptions_mod)

import poller


def iso_from_ms(value):
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


class PollerCriticalTimerTests(unittest.TestCase):
    def test_reset_uses_bounded_numeric_time_not_public_string(self):
        now_ms = poller._now_utc_ms()
        old_start_ms = now_ms - 2 * 60 * 60 * 1000
        reset_ms = now_ms - 60 * 1000
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_from_ms(old_start_ms)}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}
        resets = {"100-1": {"at_ms": reset_ms, "at": "not-a-date"}}

        poller.enrich_stations_with_port_sessions(prev, stations, resets)

        started_at = stations["100"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(started_at, iso_from_ms(reset_ms))
        poller.enrich_policy_complete_since({}, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        now_ms = poller._now_utc_ms()
        old_start = iso_from_ms(now_ms - 2 * 60 * 60 * 1000)
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}
        resets = {"100-1": {"at_ms": now_ms + 30 * 60 * 1000, "at": "not-a-date"}}

        poller.enrich_stations_with_port_sessions(prev, stations, resets)

        self.assertEqual(stations["100"]["port_sessions"]["1"]["started_at"], old_start)

    def test_invalid_persisted_start_self_heals_instead_of_crashing(self):
        prev = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev, stations, {})

        started_at = stations["100"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)

    def test_policy_enrichment_self_heals_invalid_payload_start(self):
        stations = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": "not-a-date",
                        "policy_complete_since": "2026-01-01T00:00:00+00:00",
                    }
                },
            }
        }

        poller.enrich_policy_complete_since({}, stations, {}, 120)

        session = stations["100"]["port_sessions"]["1"]
        self.assertGreater(poller._iso_to_utc_ms(session["started_at"]), 0)
        self.assertNotIn("policy_complete_since", session)

    def test_public_extension_cannot_shorten_or_exceed_cap(self):
        start_ms = poller._now_utc_ms() - 3 * 60 * 60 * 1000
        started_at = iso_from_ms(start_ms)
        base = start_ms + 120 * 60 * 1000

        shortened = poller._policy_deadline_ms(
            "100",
            "1",
            started_at,
            120,
            {"100-1": {"until_ms": start_ms + 30 * 60 * 1000}},
        )
        overextended = poller._policy_deadline_ms(
            "100",
            "1",
            started_at,
            120,
            {"100-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 60 * 60 * 1000}},
        )

        self.assertEqual(shortened, base)
        self.assertEqual(overextended, base + poller.MAX_EXTENSION_MS)

    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        prev = {
            "100": {
                "device_id": 100,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-01-01T00:00:00+00:00"}},
            }
        }
        stations = {"100": {"error": "temporary ChargePoint failure", "updated_at": "now"}}

        poller.preserve_station_state_on_fetch_errors(prev, stations)

        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            "2026-01-01T00:00:00+00:00",
        )
        self.assertTrue(stations["100"]["stale"])
        self.assertEqual(stations["100"]["last_fetch_error"], "temporary ChargePoint failure")


if __name__ == "__main__":
    unittest.main()
