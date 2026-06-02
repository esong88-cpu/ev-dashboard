import importlib
import sys
import types
import unittest


def _install_import_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda *args, **kwargs: object()

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda *args, **kwargs: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    firebase_admin.initialize_app = lambda *args, **kwargs: None

    chargepoint = types.ModuleType("python_chargepoint")

    class ChargePoint:
        pass

    chargepoint.ChargePoint = ChargePoint

    cp_exceptions = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    cp_exceptions.ChargePointCommunicationException = ChargePointCommunicationException
    cp_exceptions.ChargePointLoginError = ChargePointLoginError

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)

    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", credentials)
    sys.modules.setdefault("firebase_admin.db", db)
    sys.modules.setdefault("python_chargepoint", chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", cp_exceptions)
    sys.modules.setdefault("requests", requests)


_install_import_stubs()
poller = importlib.import_module("poller")


class PollerSessionStateTests(unittest.TestCase):
    def test_error_payload_preserves_session_for_next_successful_poll(self):
        original_start = "2020-01-01T08:00:00+00:00"
        policy_complete_since = "2020-01-01T10:00:00+00:00"
        prev_root = {
            "last_updated": "2026-06-02T10:00:00+00:00",
            "charging_limit_minutes": 120,
            "123": {
                "device_id": 123,
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {
                        "started_at": original_start,
                        "policy_complete_since": policy_complete_since,
                    }
                },
            },
        }
        failed_payload = {
            "123": {
                "error": "Station info failed for device 123: HTTP 503",
                "updated_at": "2026-06-02T10:05:00+00:00",
            }
        }

        poller.preserve_station_state_on_errors(prev_root, failed_payload)

        self.assertEqual(failed_payload["123"]["ports"], {"1": "Charging"})
        self.assertEqual(
            failed_payload["123"]["port_sessions"]["1"]["started_at"],
            original_start,
        )

        recovered_payload = {
            "123": {
                "device_id": 123,
                "ports": {"1": "Charging"},
                "updated_at": "2026-06-02T10:10:00+00:00",
            }
        }
        poller.enrich_stations_with_port_sessions(
            {"123": failed_payload["123"]}, recovered_payload, {}
        )
        poller.enrich_policy_complete_since(
            {"123": failed_payload["123"]}, recovered_payload, {}, 120
        )

        self.assertEqual(
            recovered_payload["123"]["port_sessions"]["1"]["started_at"],
            original_start,
        )
        self.assertEqual(
            recovered_payload["123"]["port_sessions"]["1"]["policy_complete_since"],
            policy_complete_since,
        )


if __name__ == "__main__":
    unittest.main()
