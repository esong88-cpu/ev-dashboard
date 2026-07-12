import sys
import types
import unittest
from datetime import datetime, timezone


def _install_dependency_stubs() -> None:
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = {}
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

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
    sys.modules.setdefault("python_chargepoint", chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_dependency_stubs()

import poller  # noqa: E402


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class PollerTimerHardeningTest(unittest.TestCase):
    def test_reset_uses_bounded_numeric_at_ms_not_public_iso_string(self) -> None:
        now_ms = poller._dt_to_ms(poller._utc_now())
        old_start_ms = now_ms - 60 * 60 * 1000
        reset_ms = now_ms - 60 * 1000
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": _iso(old_start_ms)}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        resets = {"123-1": {"at": "not an iso timestamp", "at_ms": reset_ms}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, resets)

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "not an iso timestamp")
        self.assertEqual(poller._iso_to_utc_ms(started_at), reset_ms)

        # The policy pass used to crash after the poisoned reset string was stored.
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_invalid_persisted_started_at_self_heals(self) -> None:
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "bad persisted timestamp"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(started_at, "bad persisted timestamp")
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_extension_deadline_cannot_shorten_or_exceed_cap(self) -> None:
        start_ms = poller._dt_to_ms(poller._utc_now()) - 30 * 60 * 1000
        started_at = _iso(start_ms)
        base = start_ms + 120 * 60 * 1000

        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": base - 60 * 1000}},
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
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": base + 60 * 60 * 1000}},
            ),
            base + 60 * 60 * 1000,
        )

    def test_fetch_errors_preserve_existing_station_sessions(self) -> None:
        prev_root = {
            "123": {
                "name": ["Station 123"],
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-07-12T10:00:00+00:00"}},
                "updated_at": "2026-07-12T10:05:00+00:00",
            }
        }
        stations = {
            "123": {
                "error": "temporary ChargePoint failure",
                "updated_at": "2026-07-12T10:10:00+00:00",
            }
        }

        poller.preserve_station_state_on_fetch_errors(prev_root, stations)

        self.assertEqual(stations["123"]["ports"], prev_root["123"]["ports"])
        self.assertEqual(
            stations["123"]["port_sessions"], prev_root["123"]["port_sessions"]
        )
        self.assertTrue(stations["123"]["stale"])
        self.assertEqual(
            stations["123"]["last_fetch_error"], "temporary ChargePoint failure"
        )

    def test_available_ports_retry_metadata_cleanup(self) -> None:
        stations = {"123": {"ports": {"1": "Available", "2": "Charging"}}}

        clear = poller.metadata_clear_keys_for_available_ports(
            stations,
            {"123-1": {"at_ms": 1}, "123-2": {"at_ms": 1}},
            {"123-1": {"until_ms": 2}, "123-2": {"until_ms": 2}},
        )

        self.assertEqual(clear, ["123-1"])


if __name__ == "__main__":
    unittest.main()
