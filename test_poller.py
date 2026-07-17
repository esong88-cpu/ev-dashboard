import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import poller


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


class ClientAuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def test_persisted_rotated_token_takes_priority_over_environment_bootstrap(self):
        client = object()
        with (
            patch.object(poller, "_load_saved_token", return_value="rotated-token"),
            patch.object(
                poller.ChargePoint,
                "create",
                new=AsyncMock(return_value=client),
            ) as create,
        ):
            result = await poller.get_client(
                username="driver@example.com",
                password="",
                session_token="stale-bootstrap-token",
            )

        self.assertIs(result, client)
        create.assert_awaited_once_with(
            username="driver@example.com",
            coulomb_token="rotated-token",
        )


class MainTokenPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_refreshed_token_is_saved_even_if_firebase_write_fails(self):
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

        def reference(path):
            return {
                "/stations": stations_ref,
                "/slot_resets": reset_ref,
                "/slot_extensions": extension_ref,
            }[path]

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
            patch.object(poller.db, "reference", side_effect=reference),
            patch.object(poller, "_save_token") as save_token,
        ):
            with self.assertRaisesRegex(RuntimeError, "Firebase unavailable"):
                await poller.main()

        save_token.assert_called_once_with("rotated-token")
        client.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
