import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, call, patch

import poller
from python_chargepoint.exceptions import CommunicationError


class GetClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_refreshed_persisted_token_over_bootstrap_token(self):
        client = object()
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "session-token"
            token_path.write_text("refreshed-token")
            with (
                patch.object(poller, "TOKEN_STATE_PATH", token_path),
                patch(
                    "poller.ChargePoint.create",
                    new_callable=AsyncMock,
                    return_value=client,
                ) as create,
            ):
                result = await poller.get_client(
                    "driver@example.com", "", "bootstrap-token"
                )

        self.assertIs(result, client)
        create.assert_awaited_once_with(
            username="driver@example.com", coulomb_token="refreshed-token"
        )

    async def test_falls_back_to_environment_token_when_persisted_token_is_rejected(self):
        client = object()
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "session-token"
            token_path.write_text("expired-persisted-token")
            with (
                patch.object(poller, "TOKEN_STATE_PATH", token_path),
                patch(
                    "poller.ChargePoint.create",
                    new_callable=AsyncMock,
                    side_effect=[CommunicationError("expired"), client],
                ) as create,
            ):
                result = await poller.get_client(
                    "driver@example.com", "", "fresh-bootstrap-token"
                )

        self.assertIs(result, client)
        self.assertEqual(
            create.await_args_list,
            [
                call(
                    username="driver@example.com",
                    coulomb_token="expired-persisted-token",
                ),
                call(
                    username="driver@example.com",
                    coulomb_token="fresh-bootstrap-token",
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
