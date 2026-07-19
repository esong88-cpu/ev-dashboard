import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

import poller
from python_chargepoint.exceptions import InvalidSession


class GetClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_rotated_saved_token_over_bootstrap_token(self):
        client = SimpleNamespace()
        create = AsyncMock(return_value=client)

        with (
            patch.object(poller, "_load_saved_token", return_value="rotated-token"),
            patch.object(poller.ChargePoint, "create", create),
        ):
            result = await poller.get_client(
                "driver@example.com", "", "bootstrap-token"
            )

        self.assertIs(result, client)
        create.assert_awaited_once_with(
            username="driver@example.com", coulomb_token="rotated-token"
        )

    async def test_uses_bootstrap_token_when_saved_token_expired(self):
        client = SimpleNamespace()
        create = AsyncMock(
            side_effect=[
                InvalidSession(response=None, message="expired"),
                client,
            ]
        )

        with (
            patch.object(poller, "_load_saved_token", return_value="expired-token"),
            patch.object(poller.ChargePoint, "create", create),
        ):
            result = await poller.get_client(
                "driver@example.com", "", "bootstrap-token"
            )

        self.assertIs(result, client)
        self.assertEqual(
            create.await_args_list,
            [
                call(
                    username="driver@example.com",
                    coulomb_token="expired-token",
                ),
                call(
                    username="driver@example.com",
                    coulomb_token="bootstrap-token",
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
