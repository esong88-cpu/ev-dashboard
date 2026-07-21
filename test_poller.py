import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call, patch


# Keep these focused tests runnable without installing the production services.
firebase_admin = types.ModuleType("firebase_admin")
firebase_admin._apps = []
firebase_admin.credentials = SimpleNamespace()
firebase_admin.db = SimpleNamespace()
sys.modules["firebase_admin"] = firebase_admin

chargepoint_module = types.ModuleType("python_chargepoint")
chargepoint_exceptions = types.ModuleType("python_chargepoint.exceptions")


class CommunicationError(Exception):
    pass


class DatadomeCaptcha(Exception):
    pass


class LoginError(Exception):
    pass


class ChargePoint:
    create = AsyncMock()


chargepoint_module.ChargePoint = ChargePoint
chargepoint_exceptions.CommunicationError = CommunicationError
chargepoint_exceptions.DatadomeCaptcha = DatadomeCaptcha
chargepoint_exceptions.LoginError = LoginError
sys.modules["python_chargepoint"] = chargepoint_module
sys.modules["python_chargepoint.exceptions"] = chargepoint_exceptions

import poller


class SessionTokenSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.original_token_path = poller.TOKEN_STATE_PATH
        self.addCleanup(setattr, poller, "TOKEN_STATE_PATH", self.original_token_path)
        poller.TOKEN_STATE_PATH = Path(self.temp_dir.name) / "token"

    async def test_persisted_rotating_token_wins_over_environment_bootstrap(self):
        poller.TOKEN_STATE_PATH.write_text("rotated-token")
        client = object()

        with patch.object(
            poller.ChargePoint, "create", AsyncMock(return_value=client)
        ) as create:
            result = await poller.get_client("driver", "", "bootstrap-token")

        self.assertIs(result, client)
        create.assert_awaited_once_with(
            username="driver", coulomb_token="rotated-token"
        )

    async def test_rejected_persisted_token_falls_back_to_environment_token(self):
        poller.TOKEN_STATE_PATH.write_text("stale-token")
        client = object()

        with patch.object(
            poller.ChargePoint,
            "create",
            AsyncMock(side_effect=[CommunicationError("expired"), client]),
        ) as create:
            result = await poller.get_client("driver", "", "bootstrap-token")

        self.assertIs(result, client)
        self.assertEqual(
            create.await_args_list,
            [
                call(username="driver", coulomb_token="stale-token"),
                call(username="driver", coulomb_token="bootstrap-token"),
            ],
        )


class TokenPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rotated_token_is_saved_when_firebase_write_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "token"
            client = SimpleNamespace(
                coulomb_token="new-rotated-token",
                get_home_chargers=AsyncMock(return_value=[]),
                close=AsyncMock(),
            )
            stations_ref = Mock()
            stations_ref.get.return_value = {}
            stations_ref.set.side_effect = RuntimeError("Firebase unavailable")
            reset_ref = Mock()
            reset_ref.get.return_value = {}
            extension_ref = Mock()
            extension_ref.get.return_value = {}
            refs = {
                "/stations": stations_ref,
                "/slot_resets": reset_ref,
                "/slot_extensions": extension_ref,
            }
            env = {
                "CHARGEPOINT_USER": "driver",
                "CHARGEPOINT_SESSION_TOKEN": "bootstrap-token",
                "CHARGEPOINT_STATION_IDS": "123",
                "FIREBASE_DATABASE_URL": "https://example.invalid",
            }

            with (
                patch.dict(os.environ, env, clear=True),
                patch.object(poller, "TOKEN_STATE_PATH", token_path),
                patch.object(poller, "init_firebase"),
                patch.object(
                    poller, "get_client", AsyncMock(return_value=client)
                ),
                patch.object(poller, "poll_once", AsyncMock(return_value={})),
                patch.object(
                    poller.db,
                    "reference",
                    side_effect=lambda path: refs[path],
                    create=True,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "Firebase unavailable"):
                    await poller.main()

            self.assertEqual(token_path.read_text(), "new-rotated-token")
            client.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
