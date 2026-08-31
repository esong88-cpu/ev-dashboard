import importlib
import sys
import types
import unittest
from unittest import mock


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda *_args, **_kwargs: object()

    db = types.ModuleType("firebase_admin.db")
    db.reference = mock.Mock()

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    firebase_admin.initialize_app = mock.Mock()

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)

    python_chargepoint = types.ModuleType("python_chargepoint")

    class ChargePoint:
        pass

    python_chargepoint.ChargePoint = ChargePoint

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
    sys.modules.setdefault("requests", requests)
    sys.modules.setdefault("python_chargepoint", python_chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_dependency_stubs()
poller = importlib.import_module("poller")


class StationIdParsingTest(unittest.TestCase):
    def test_parse_station_ids_trims_tokens(self):
        self.assertEqual(poller._parse_station_ids(" 101,202 , 303 "), [101, 202, 303])

    def test_parse_station_ids_rejects_invalid_token(self):
        with self.assertRaisesRegex(ValueError, "Invalid station ID 'abc'"):
            poller._parse_station_ids("101,abc,303")

    def test_main_exits_before_firebase_init_for_empty_station_id_list(self):
        env = {
            "CHARGEPOINT_USER": "user@example.com",
            "CHARGEPOINT_PASS": "password",
            "CHARGEPOINT_SESSION_TOKEN": "",
            "CHARGEPOINT_STATION_IDS": " , ,, ",
            "FIREBASE_DATABASE_URL": "https://example.firebaseio.com",
            "FIREBASE_KEY": "{}",
        }
        with mock.patch.dict(poller.os.environ, env, clear=True):
            with mock.patch.object(poller, "init_firebase") as init_firebase:
                with self.assertRaises(SystemExit) as ctx:
                    poller.main()
        self.assertEqual(ctx.exception.code, 1)
        init_firebase.assert_not_called()

    def test_main_exits_before_firebase_init_for_malformed_station_id(self):
        env = {
            "CHARGEPOINT_USER": "user@example.com",
            "CHARGEPOINT_PASS": "password",
            "CHARGEPOINT_SESSION_TOKEN": "",
            "CHARGEPOINT_STATION_IDS": "101,abc",
            "FIREBASE_DATABASE_URL": "https://example.firebaseio.com",
            "FIREBASE_KEY": "{}",
        }
        with mock.patch.dict(poller.os.environ, env, clear=True):
            with mock.patch.object(poller, "init_firebase") as init_firebase:
                with self.assertRaises(SystemExit) as ctx:
                    poller.main()
        self.assertEqual(ctx.exception.code, 1)
        init_firebase.assert_not_called()


if __name__ == "__main__":
    unittest.main()
