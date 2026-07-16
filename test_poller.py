import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

import poller


class SessionTokenTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.token_path = Path(self.temp_dir.name) / "chargepoint-token"
        token_path_patch = patch.object(poller, "TOKEN_STATE_PATH", self.token_path)
        token_path_patch.start()
        self.addCleanup(token_path_patch.stop)

    async def test_persisted_token_takes_precedence_over_environment_bootstrap(self):
        self.token_path.write_text("rotated-token")
        client = SimpleNamespace()

        with patch.object(
            poller.ChargePoint,
            "create",
            AsyncMock(return_value=client),
        ) as create:
            actual = await poller.get_client("user", "password", "bootstrap-token")

        self.assertIs(actual, client)
        create.assert_awaited_once_with(
            username="user",
            coulomb_token="rotated-token",
        )

    async def test_environment_token_is_tried_when_persisted_token_is_rejected(self):
        self.token_path.write_text("expired-token")
        client = SimpleNamespace()

        with patch.object(
            poller.ChargePoint,
            "create",
            AsyncMock(
                side_effect=[
                    poller.CommunicationError("expired"),
                    client,
                ]
            ),
        ) as create:
            actual = await poller.get_client("user", "password", "bootstrap-token")

        self.assertIs(actual, client)
        self.assertEqual(
            create.await_args_list,
            [
                call(
                    username="user",
                    coulomb_token="expired-token",
                ),
                call(
                    username="user",
                    coulomb_token="bootstrap-token",
                ),
            ],
        )

    async def test_refreshed_token_is_persisted_even_when_close_fails(self):
        client = SimpleNamespace(
            coulomb_token="refreshed-token",
            close=AsyncMock(side_effect=RuntimeError("close failed")),
        )

        with self.assertRaisesRegex(RuntimeError, "close failed"):
            await poller._persist_token_and_close(client)

        self.assertEqual(self.token_path.read_text(), "refreshed-token")
        client.close.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
