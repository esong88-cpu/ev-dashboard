import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import poller


def invalid_session() -> poller.InvalidSession:
    return poller.InvalidSession(response=None, message="expired session")


class TokenPersistenceTests(unittest.TestCase):
    def test_save_token_replaces_file_with_private_permissions(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            token_path = Path(temporary_directory) / "session-token"
            token_path.write_text("stale")
            token_path.chmod(0o644)

            with patch.object(poller, "TOKEN_STATE_PATH", token_path):
                poller._save_token("rotated")

            self.assertEqual(token_path.read_text(), "rotated")
            self.assertEqual(stat.S_IMODE(token_path.stat().st_mode), 0o600)


class ClientCreationTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_client_setup_closes_owned_session(self):
        session = MagicMock()
        session.close = AsyncMock()

        with (
            patch.object(poller.aiohttp, "ClientSession", return_value=session),
            patch.object(
                poller.ChargePoint,
                "create",
                new=AsyncMock(side_effect=invalid_session()),
            ),
        ):
            with self.assertRaises(poller.InvalidSession):
                await poller._create_client("driver@example.com", "expired-token")

        session.close.assert_awaited_once_with()


class ClientAuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def test_persisted_token_takes_priority_over_environment_bootstrap(self):
        client = object()
        with (
            patch.object(poller, "_load_saved_token", return_value="rotated-token"),
            patch.object(
                poller,
                "_create_client",
                new=AsyncMock(return_value=client),
            ) as create,
        ):
            result = await poller.get_client(
                "driver@example.com",
                "",
                "stale-bootstrap-token",
            )

        self.assertIs(result, client)
        create.assert_awaited_once_with("driver@example.com", "rotated-token")

    async def test_environment_token_follows_rejected_persisted_token(self):
        client = object()
        with (
            patch.object(poller, "_load_saved_token", return_value="expired-token"),
            patch.object(
                poller,
                "_create_client",
                new=AsyncMock(side_effect=[invalid_session(), client]),
            ) as create,
        ):
            result = await poller.get_client(
                "driver@example.com",
                "",
                "bootstrap-token",
            )

        self.assertIs(result, client)
        self.assertEqual(
            create.await_args_list,
            [
                call("driver@example.com", "expired-token"),
                call("driver@example.com", "bootstrap-token"),
            ],
        )

    async def test_transient_communication_error_does_not_trigger_auth_fallback(self):
        error = poller.CommunicationError(response=None, message="service unavailable")
        with (
            patch.object(poller, "_load_saved_token", return_value="rotated-token"),
            patch.object(
                poller,
                "_create_client",
                new=AsyncMock(side_effect=error),
            ) as create,
        ):
            with self.assertRaises(poller.CommunicationError):
                await poller.get_client(
                    "driver@example.com",
                    "password",
                    "bootstrap-token",
                )

        create.assert_awaited_once_with("driver@example.com", "rotated-token")

    async def test_failed_password_login_closes_client(self):
        client = SimpleNamespace(
            login_with_password=AsyncMock(
                side_effect=poller.LoginError(response=None, message="bad password")
            ),
            close=AsyncMock(),
        )
        with (
            patch.object(poller, "_load_saved_token", return_value=""),
            patch.object(
                poller,
                "_create_client",
                new=AsyncMock(return_value=client),
            ),
        ):
            with self.assertRaises(poller.LoginError):
                await poller.get_client("driver@example.com", "password", "")

        client.close.assert_awaited_once_with()


class MainTokenPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_refreshed_token_is_saved_when_firebase_write_fails(self):
        client = MagicMock()
        client.coulomb_token = "rotated-token"
        client.get_home_chargers = AsyncMock(return_value=[])
        client.close = AsyncMock()

        stations_ref = MagicMock()
        stations_ref.get.return_value = {}
        stations_ref.set.side_effect = RuntimeError("Firebase unavailable")

        reset_ref = MagicMock()
        reset_ref.get.return_value = {}
        extension_ref = MagicMock()
        extension_ref.get.return_value = {}

        references = {
            "/stations": stations_ref,
            "/slot_resets": reset_ref,
            "/slot_extensions": extension_ref,
        }
        environment = {
            "CHARGEPOINT_USER": "driver@example.com",
            "CHARGEPOINT_PASS": "password",
            "CHARGEPOINT_SESSION_TOKEN": "bootstrap-token",
            "CHARGEPOINT_STATION_IDS": "123",
            "FIREBASE_DATABASE_URL": "https://example.firebaseio.com",
        }

        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(poller, "init_firebase"),
            patch.object(poller, "get_client", new=AsyncMock(return_value=client)),
            patch.object(poller, "poll_once", new=AsyncMock(return_value={})),
            patch.object(
                poller.db,
                "reference",
                side_effect=lambda path: references[path],
            ),
            patch.object(poller, "_save_token") as save_token,
        ):
            with self.assertRaisesRegex(RuntimeError, "Firebase unavailable"):
                await poller.main()

        save_token.assert_called_once_with("rotated-token")
        client.close.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
