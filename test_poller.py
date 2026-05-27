import sys
import types
import unittest


def _install_import_stubs():
    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)
    sys.modules.setdefault("requests", requests)

    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda *_args, **_kwargs: object())
    firebase_admin.db = types.SimpleNamespace(reference=lambda *_args, **_kwargs: None)
    firebase_admin.initialize_app = lambda *_args, **_kwargs: None
    sys.modules.setdefault("firebase_admin", firebase_admin)

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = object
    sys.modules.setdefault("python_chargepoint", chargepoint)

    exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions.ChargePointLoginError = ChargePointLoginError
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_import_stubs()

import poller  # noqa: E402


class PollerFailurePreservationTest(unittest.TestCase):
    def test_carry_forward_failed_station_preserves_previous_session(self):
        prev_root = {
            "last_updated": "2026-05-27T10:00:00+00:00",
            "123": {
                "device_id": 123,
                "name": ["Station 123"],
                "ports": [None, "Charging"],
                "port_sessions": [None, {"started_at": "2026-05-27T09:00:00+00:00"}],
                "updated_at": "2026-05-27T09:00:00+00:00",
            },
        }
        stations = {
            "123": {
                "error": "ChargePoint timeout",
                "updated_at": "2026-05-27T10:05:00+00:00",
            }
        }

        carried = poller._carry_forward_failed_stations(prev_root, stations)
        cleared_ext = poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual(carried, 1)
        self.assertEqual(cleared_ext, [])
        self.assertNotIn("error", stations["123"])
        self.assertTrue(stations["123"]["stale"])
        self.assertEqual(stations["123"]["poll_error"], "ChargePoint timeout")
        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-05-27T09:00:00+00:00",
        )

    def test_failed_station_without_previous_ports_stays_error(self):
        stations = {
            "123": {
                "error": "ChargePoint timeout",
                "updated_at": "2026-05-27T10:05:00+00:00",
            }
        }

        carried = poller._carry_forward_failed_stations({}, stations)

        self.assertEqual(carried, 0)
        self.assertEqual(stations["123"]["error"], "ChargePoint timeout")

    def test_rtdb_map_preserves_array_indices(self):
        self.assertEqual(
            poller._rtdb_map([None, "Available", "Charging"]),
            {"1": "Available", "2": "Charging"},
        )

    def test_enrich_ignores_public_reset_from_far_future(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-05-27T09:00:00+00:00"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {
            "123-1": {
                "at": "not a date",
                "at_ms": 32503680000000,
            }
        }

        poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-05-27T09:00:00+00:00",
        )

    def test_enrich_derives_reset_iso_from_valid_reset_ms(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-05-27T09:00:00+00:00"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_ms = 1780000000000
        reset_map = {
            "123-1": {
                "at": "not a date",
                "at_ms": reset_ms,
            }
        }
        original_datetime = poller.datetime

        class FixedDateTime(original_datetime):
            @classmethod
            def now(cls, tz=None):
                return original_datetime.fromtimestamp(reset_ms / 1000, tz=poller.timezone.utc)

        try:
            poller.datetime = FixedDateTime
            poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)
        finally:
            poller.datetime = original_datetime

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            original_datetime.fromtimestamp(reset_ms / 1000, tz=poller.timezone.utc).isoformat(),
        )


if __name__ == "__main__":
    unittest.main()
