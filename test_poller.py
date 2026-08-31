import sys
import types
import unittest


def _install_import_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda *_args, **_kwargs: object())
    firebase_admin.db = types.SimpleNamespace(reference=lambda *_args, **_kwargs: None)
    firebase_admin.initialize_app = lambda *_args, **_kwargs: None
    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)

    chargepoint_mod = types.ModuleType("python_chargepoint")
    chargepoint_mod.ChargePoint = object
    exceptions_mod = types.ModuleType("python_chargepoint.exceptions")

    class ChargePointCommunicationException(Exception):
        pass

    class ChargePointLoginError(Exception):
        pass

    exceptions_mod.ChargePointCommunicationException = ChargePointCommunicationException
    exceptions_mod.ChargePointLoginError = ChargePointLoginError
    sys.modules.setdefault("python_chargepoint", chargepoint_mod)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions_mod)


_install_import_stubs()

import poller


class PollerTimerTests(unittest.TestCase):
    def test_invalid_iso_returns_zero_instead_of_raising(self):
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)

    def test_reset_uses_numeric_timestamp_not_user_provided_iso(self):
        prev = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-05-04T10:00:00+00:00"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_ms = 1777892400000
        resets = {
            "123-1": {
                "at": "not-a-date",
                "at_ms": reset_ms,
            }
        }

        poller.enrich_stations_with_port_sessions(prev, stations, resets)

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(started_at, "2026-05-04T11:00:00+00:00")
        self.assertNotEqual(started_at, resets["123-1"]["at"])
        self.assertGreater(poller._iso_to_utc_ms(started_at), 0)


if __name__ == "__main__":
    unittest.main()
