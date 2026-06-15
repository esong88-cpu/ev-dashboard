import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.initialize_app = lambda *args, **kwargs: None

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda value: value

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda path: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", credentials)
    sys.modules.setdefault("firebase_admin.db", db)

    python_chargepoint = types.ModuleType("python_chargepoint")

    class ChargePoint:
        pass

    python_chargepoint.ChargePoint = ChargePoint
    sys.modules.setdefault("python_chargepoint", python_chargepoint)

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
poller = importlib.import_module("poller")


class PollerTimerHardeningTests(unittest.TestCase):
    def test_fetch_error_preserves_last_known_station_state(self):
        prev_root = {
            "100": {
                "device_id": 100,
                "name": ["Lot A"],
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": "2026-06-14T08:00:00+00:00",
                        "policy_complete_since": "2026-06-14T10:00:00+00:00",
                    }
                },
                "updated_at": "2026-06-14T10:00:00+00:00",
            }
        }
        stations = {
            "100": {
                "error": "timeout",
                "updated_at": "2026-06-14T10:05:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertNotIn("error", stations["100"])
        self.assertEqual(stations["100"]["ports"], {"1": "Charging"})
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            "2026-06-14T08:00:00+00:00",
        )
        self.assertEqual(stations["100"]["last_fetch_error"], "timeout")
        self.assertEqual(
            stations["100"]["last_fetch_error_at"], "2026-06-14T10:05:00+00:00"
        )
        self.assertTrue(stations["100"]["stale"])

    def test_invalid_started_at_self_heals_without_crashing(self):
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})
        started_at = stations["100"]["port_sessions"]["1"]["started_at"]

        self.assertNotEqual(started_at, "not-a-date")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_reset_uses_bounded_numeric_timestamp_and_clears_metadata(self):
        old_start = datetime.now(timezone.utc) - timedelta(hours=3)
        reset_ms = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        cleared = poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"100-1": {"at": "not-a-date", "at_ms": reset_ms}},
        )

        self.assertEqual(cleared, ["100-1"])
        self.assertEqual(
            stations["100"]["port_sessions"]["1"]["started_at"],
            datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat(),
        )

    def test_reset_rejects_far_future_timestamp(self):
        old_start = datetime.now(timezone.utc) - timedelta(hours=3)
        future_ms = int(
            (datetime.now(timezone.utc) + timedelta(hours=2)).timestamp() * 1000
        )
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start.isoformat()}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        cleared = poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"100-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": future_ms}},
        )

        self.assertEqual(cleared, [])
        self.assertEqual(stations["100"]["port_sessions"]["1"]["started_at"], old_start.isoformat())

    def test_policy_deadline_ignores_shortening_and_implausible_extensions(self):
        start = datetime(2026, 6, 14, 8, 0, tzinfo=timezone.utc)
        start_iso = start.isoformat()
        base = poller._iso_to_utc_ms(start_iso) + 120 * 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", start_iso, 120, {"100-1": {"until_ms": base - 1}}
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "100",
                "1",
                start_iso,
                120,
                {"100-1": {"until_ms": base + poller.MAX_EXTENSION_MS + 1}},
            ),
            base,
        )
        self.assertEqual(
            poller._policy_deadline_ms(
                "100", "1", start_iso, 120, {"100-1": {"until_ms": base + 1}}
            ),
            base + 1,
        )

    def test_invalid_iso_returns_zero(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)


if __name__ == "__main__":
    unittest.main()
