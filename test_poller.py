import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, call, patch

import poller
from python_chargepoint.exceptions import CommunicationError


class GetClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_rotated_persisted_token_over_bootstrap_token(self):
        expected_client = object()
        create = AsyncMock(return_value=expected_client)

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "session-token"
            token_path.write_text("rotated-token")
            with (
                patch.object(poller, "TOKEN_STATE_PATH", token_path),
                patch.object(poller.ChargePoint, "create", create),
            ):
                client = await poller.get_client(
                    "user@example.com", "", "bootstrap-token"
                )

        self.assertIs(client, expected_client)
        create.assert_awaited_once_with(
            username="user@example.com", coulomb_token="rotated-token"
        )

    async def test_falls_back_to_bootstrap_token_when_persisted_token_is_rejected(self):
        expected_client = object()
        create = AsyncMock(
            side_effect=[
                CommunicationError(None, "expired token"),
                expected_client,
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "session-token"
            token_path.write_text("expired-token")
            with (
                patch.object(poller, "TOKEN_STATE_PATH", token_path),
                patch.object(poller.ChargePoint, "create", create),
            ):
                client = await poller.get_client(
                    "user@example.com", "", "bootstrap-token"
                )

        self.assertIs(client, expected_client)
        self.assertEqual(
            create.await_args_list,
            [
                call(
                    username="user@example.com",
                    coulomb_token="expired-token",
                ),
                call(
                    username="user@example.com",
                    coulomb_token="bootstrap-token",
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
