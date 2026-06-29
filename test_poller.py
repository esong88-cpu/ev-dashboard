import sys
import types
import unittest
from datetime import datetime, timezone


firebase_admin_stub = types.ModuleType("firebase_admin")
firebase_admin_stub._apps = []
firebase_admin_stub.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin_stub.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin_stub)
sys.modules.setdefault("firebase_admin.credentials", firebase_admin_stub.credentials)
sys.modules.setdefault("firebase_admin.db", firebase_admin_stub.db)

chargepoint_stub = types.ModuleType("python_chargepoint")
chargepoint_stub.ChargePoint = object
exceptions_stub = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions_stub.ChargePointCommunicationException = ChargePointCommunicationException
exceptions_stub.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", chargepoint_stub)
sys.modules.setdefault("python_chargepoint.exceptions", exceptions_stub)

import poller


def iso_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class PollerTimerHardeningTest(unittest.TestCase):
    def test_reset_uses_bounded_numeric_time_not_public_iso_string(self):
        now_ms = poller._now_utc_ms()
        old_start_ms = now_ms - 20 * 60 * 1000
        reset_ms = now_ms - 2 * 60 * 1000
        prev = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": iso_from_ms(old_start_ms)}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev,
            stations,
            {"101-1": {"at_ms": reset_ms, "at": "not-a-date"}},
        )

        started_at = stations["101"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "not-a-date")
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

    def test_far_future_reset_is_ignored(self):
        now_ms = poller._now_utc_ms()
        old_start_ms = now_ms - 20 * 60 * 1000
        future_reset_ms = now_ms + poller.MAX_RESET_FUTURE_SKEW_MS + 60 * 1000
        old_start = iso_from_ms(old_start_ms)
        prev = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev,
            stations,
            {"101-1": {"at_ms": future_reset_ms, "at": "2099-01-01T00:00:00+00:00"}},
        )

        self.assertEqual(stations["101"]["port_sessions"]["1"]["started_at"], old_start)

    def test_invalid_persisted_started_at_self_heals_before_policy_enrichment(self):
        prev = {
            "101": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "invalid-timestamp"}},
            }
        }
        stations = {"101": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev, stations, {})
        poller.enrich_policy_complete_since(prev, stations, {}, 120)

        started_at = stations["101"]["port_sessions"]["1"]["started_at"]
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)

    def test_extension_deadline_cannot_shorten_or_exceed_cap(self):
        start_ms = poller._now_utc_ms() - 2 * 60 * 60 * 1000
        base = start_ms + 120 * 60 * 1000
        self.assertEqual(
            poller._policy_deadline_ms(
                "101",
                "1",
                iso_from_ms(start_ms),
                120,
                {"101-1": {"until_ms": base - 60 * 1000}},
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "101",
                "1",
                iso_from_ms(start_ms),
                120,
                {"101-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 1}},
            ),
            base,
        )
        valid_until = base + 30 * 60 * 1000
        self.assertEqual(
            poller._policy_deadline_ms(
                "101",
                "1",
                iso_from_ms(start_ms),
                120,
                {"101-1": {"until_ms": valid_until}},
            ),
            valid_until,
        )

    def test_fetch_error_preserves_previous_ports_and_sessions(self):
        prev = {
            "101": {
                "device_id": 101,
                "name": ["Station"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-06-29T10:00:00+00:00"}},
            }
        }
        stations = {"101": {"error": "temporary API failure", "updated_at": "now"}}

        poller.preserve_station_state_on_fetch_errors(prev, stations)

        self.assertNotIn("error", stations["101"])
        self.assertEqual(stations["101"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["101"]["port_sessions"],
            {"1": {"started_at": "2026-06-29T10:00:00+00:00"}},
        )
        self.assertEqual(stations["101"]["last_fetch_error"], "temporary API failure")


if __name__ == "__main__":
    unittest.main()
