import sys
import types
import unittest


firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.initialize_app = lambda *args, **kwargs: None
firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
sys.modules.setdefault("firebase_admin", firebase_admin)

python_chargepoint = types.ModuleType("python_chargepoint")
python_chargepoint.ChargePoint = object
exceptions = types.ModuleType("python_chargepoint.exceptions")


class ChargePointCommunicationException(Exception):
    pass


class ChargePointLoginError(Exception):
    pass


exceptions.ChargePointCommunicationException = ChargePointCommunicationException
exceptions.ChargePointLoginError = ChargePointLoginError
sys.modules.setdefault("python_chargepoint", python_chargepoint)
sys.modules.setdefault("python_chargepoint.exceptions", exceptions)

import poller


class PreserveFailedStationPayloadsTest(unittest.TestCase):
    def test_preserves_last_good_station_state_on_fetch_error(self):
        previous_start = "2026-05-19T10:00:00+00:00"
        previous_root = {
            "last_updated": "2026-05-19T10:05:00+00:00",
            "123": {
                "device_id": 123,
                "name": ["Station 123"],
                "source": "public",
                "ports": [None, "Charging", "Available"],
                "port_sessions": [None, {"started_at": previous_start}],
                "updated_at": "2026-05-19T10:05:00+00:00",
            },
        }
        stations = {
            "123": {
                "error": "timeout",
                "updated_at": "2026-05-19T10:10:00+00:00",
            }
        }

        poller.preserve_failed_station_payloads(previous_root, stations)
        cleared = poller.enrich_stations_with_port_sessions(previous_root, stations, {})

        self.assertEqual([], cleared)
        self.assertNotIn("error", stations["123"])
        self.assertEqual("timeout", stations["123"]["fetch_error"])
        self.assertTrue(stations["123"]["stale"])
        self.assertEqual({"1": "Charging", "2": "Available"}, stations["123"]["ports"])
        self.assertEqual(
            previous_start,
            stations["123"]["port_sessions"]["1"]["started_at"],
        )

    def test_leaves_error_when_no_previous_ports_exist(self):
        stations = {
            "123": {
                "error": "timeout",
                "updated_at": "2026-05-19T10:10:00+00:00",
            }
        }

        poller.preserve_failed_station_payloads({}, stations)

        self.assertEqual("timeout", stations["123"]["error"])


if __name__ == "__main__":
    unittest.main()
