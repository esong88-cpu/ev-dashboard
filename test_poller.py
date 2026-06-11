import importlib
import sys
import types
import unittest
from datetime import datetime, timezone


def _install_import_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.initialize_app = lambda *args, **kwargs: None

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda value: value

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda path: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db

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

    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", credentials)
    sys.modules.setdefault("firebase_admin.db", db)
    sys.modules.setdefault("python_chargepoint", chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_import_stubs()
poller = importlib.import_module("poller")


def iso_ms(raw):
    return poller._iso_to_utc_ms(raw)


class PollerHardeningTest(unittest.TestCase):
    def test_bad_iso_returns_zero_instead_of_raising(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)

    def test_reset_ignores_untrusted_iso_and_uses_bounded_at_ms(self):
        start = "2026-06-11T10:00:00+00:00"
        reset_ms = iso_ms("2026-06-11T10:05:00+00:00")
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {"123-1": {"at": "not-a-date", "at_ms": reset_ms}}

        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(cleared, [])
        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat(),
        )

    def test_future_reset_does_not_push_session_start_forward(self):
        start = "2026-06-11T10:00:00+00:00"
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {"123-1": {"at_ms": 4102444800000}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], start)

    def test_invalid_persisted_session_start_self_heals(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "bad timestamp"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        self.assertNotEqual(started_at, "bad timestamp")

    def test_policy_deadline_ignores_shortening_and_unbounded_extensions(self):
        started_at = "2026-06-11T08:00:00+00:00"
        start_ms = iso_ms(started_at)
        base = start_ms + 120 * 60 * 1000
        now_ms = iso_ms("2026-06-11T09:00:00+00:00")

        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": base - 1}},
                now_ms,
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": 4102444800000}},
                now_ms,
            ),
            base,
        )

        valid_until = base + 15 * 60 * 1000
        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": valid_until}},
                now_ms,
            ),
            valid_until,
        )

    def test_preserve_station_state_on_fetch_error_keeps_session_clock(self):
        prev_root = {
            "123": {
                "device_id": 123,
                "name": ["Station 123"],
                "station_status": "online",
                "source": "public",
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-06-11T10:00:00+00:00"}
                },
            }
        }
        stations = {"123": {"error": "timeout", "updated_at": "2026-06-11T10:05:00+00:00"}}

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertEqual(stations["123"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-06-11T10:00:00+00:00",
        )
        self.assertTrue(stations["123"]["stale"])


if __name__ == "__main__":
    unittest.main()
