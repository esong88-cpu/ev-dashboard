import unittest
from unittest.mock import AsyncMock, call, patch

import poller
from python_chargepoint.exceptions import CommunicationError


def rejected_token() -> CommunicationError:
    return CommunicationError(response=None, message="expired session")


class GetClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_rotated_saved_token_over_bootstrap_environment_token(self):
        client = object()
        with (
            patch.object(poller, "_load_saved_token", return_value="rotated-token"),
            patch.object(
                poller.ChargePoint,
                "create",
                new_callable=AsyncMock,
                return_value=client,
            ) as create,
        ):
            result = await poller.get_client("user", "", "bootstrap-token")

        self.assertIs(result, client)
        create.assert_awaited_once_with(
            username="user", coulomb_token="rotated-token"
        )

    async def test_tries_bootstrap_token_when_saved_token_is_rejected(self):
        client = object()
        with (
            patch.object(poller, "_load_saved_token", return_value="expired-token"),
            patch.object(
                poller.ChargePoint,
                "create",
                new_callable=AsyncMock,
                side_effect=[rejected_token(), client],
            ) as create,
        ):
            result = await poller.get_client("user", "", "bootstrap-token")

        self.assertIs(result, client)
        self.assertEqual(
            create.await_args_list,
            [
                call(username="user", coulomb_token="expired-token"),
                call(username="user", coulomb_token="bootstrap-token"),
            ],
        )

    async def test_uses_password_only_after_all_session_tokens_are_rejected(self):
        password_client = unittest.mock.Mock()
        password_client.login_with_password = AsyncMock()
        with (
            patch.object(poller, "_load_saved_token", return_value="expired-token"),
            patch.object(
                poller.ChargePoint,
                "create",
                new_callable=AsyncMock,
                side_effect=[
                    rejected_token(),
                    rejected_token(),
                    password_client,
                ],
            ) as create,
        ):
            result = await poller.get_client("user", "password", "bootstrap-token")

        self.assertIs(result, password_client)
        self.assertEqual(
            create.await_args_list,
            [
                call(username="user", coulomb_token="expired-token"),
                call(username="user", coulomb_token="bootstrap-token"),
                call(username="user"),
            ],
        )
        password_client.login_with_password.assert_awaited_once_with("password")


if __name__ == "__main__":
    unittest.main()
