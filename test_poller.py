import sys
import types
import unittest


def _install_dependency_stubs() -> None:
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda *_args, **_kwargs: object()

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda *_args, **_kwargs: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    firebase_admin.initialize_app = lambda *_args, **_kwargs: None

    chargepoint = types.ModuleType("python_chargepoint")

    class ChargePoint:  # pragma: no cover - import shim only
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


_install_dependency_stubs()

import poller  # noqa: E402


class PollerStatePreservationTests(unittest.TestCase):
    def test_rtdb_map_preserves_actual_array_indexes(self) -> None:
        self.assertEqual(
            poller._rtdb_map([None, "Available", "Charging"]),
            {"1": "Available", "2": "Charging"},
        )

    def test_fetch_error_preserves_previous_station_and_sessions(self) -> None:
        previous = {
            "123": {
                "device_id": 123,
                "name": ["Station 123"],
                "ports": [None, "Charging"],
                "port_sessions": [
                    None,
                    {
                        "started_at": "2026-05-20T09:00:00+00:00",
                        "policy_complete_since": "2026-05-20T11:00:00+00:00",
                    },
                ],
                "updated_at": "2026-05-20T09:05:00+00:00",
            }
        }
        current = {
            "123": {
                "error": "Station info failed: HTTP 502",
                "updated_at": "2026-05-20T11:05:00+00:00",
            }
        }

        poller.preserve_previous_station_on_errors(previous, current)

        station = current["123"]
        self.assertNotIn("error", station)
        self.assertEqual(station["ports"], {"1": "Charging"})
        self.assertEqual(
            station["port_sessions"],
            {
                "1": {
                    "started_at": "2026-05-20T09:00:00+00:00",
                    "policy_complete_since": "2026-05-20T11:00:00+00:00",
                }
            },
        )
        self.assertEqual(station["last_fetch_error"], "Station info failed: HTTP 502")
        self.assertEqual(station["last_fetch_error_at"], "2026-05-20T11:05:00+00:00")

    def test_preserved_station_does_not_restart_occupied_session(self) -> None:
        previous = {
            "123": {
                "ports": [None, "Charging"],
                "port_sessions": [
                    None,
                    {"started_at": "2026-05-20T09:00:00+00:00"},
                ],
            }
        }
        current = {
            "123": {
                "error": "timeout",
                "updated_at": "2026-05-20T11:05:00+00:00",
            }
        }

        poller.preserve_previous_station_on_errors(previous, current)
        poller.enrich_stations_with_port_sessions(previous, current, {})

        self.assertEqual(
            current["123"]["port_sessions"]["1"]["started_at"],
            "2026-05-20T09:00:00+00:00",
        )


if __name__ == "__main__":
    unittest.main()
