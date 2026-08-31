import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


class _FakeCredentials:
    @staticmethod
    def Certificate(_info):
        return object()


class _FakeDb:
    @staticmethod
    def reference(_path):
        raise AssertionError("Firebase should not be used by unit tests")


class _FakeChargePoint:
    pass


class _FakeChargePointCommunicationException(Exception):
    pass


class _FakeChargePointLoginError(Exception):
    pass


firebase_admin = types.SimpleNamespace(
    _apps=[],
    credentials=_FakeCredentials,
    db=_FakeDb,
    initialize_app=lambda *_args, **_kwargs: None,
)
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.credentials", _FakeCredentials)
sys.modules.setdefault("firebase_admin.db", _FakeDb)
sys.modules.setdefault(
    "python_chargepoint",
    types.SimpleNamespace(ChargePoint=_FakeChargePoint),
)
sys.modules.setdefault(
    "python_chargepoint.exceptions",
    types.SimpleNamespace(
        ChargePointCommunicationException=_FakeChargePointCommunicationException,
        ChargePointLoginError=_FakeChargePointLoginError,
    ),
)

import poller


class ManualResetTests(unittest.TestCase):
    def test_recent_reset_restarts_occupied_session(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        reset_at = datetime.now(timezone.utc) - timedelta(minutes=2)
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            {
                "123": {
                    "ports": {"1": "Charging"},
                    "port_sessions": {"1": {"started_at": old_start}},
                }
            },
            stations,
            {
                "123-1": {
                    "at_ms": int(reset_at.timestamp() * 1000),
                    "at": reset_at.isoformat(),
                }
            },
        )

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertGreaterEqual(poller._iso_to_utc_ms(started_at), int(reset_at.timestamp() * 1000))

    def test_future_reset_is_ignored(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        future_reset = datetime.now(timezone.utc) + timedelta(days=365)
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            {
                "123": {
                    "ports": {"1": "Charging"},
                    "port_sessions": {"1": {"started_at": old_start}},
                }
            },
            stations,
            {
                "123-1": {
                    "at_ms": int(future_reset.timestamp() * 1000),
                    "at": future_reset.isoformat(),
                }
            },
        )

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], old_start)


class ExtensionDeadlineTests(unittest.TestCase):
    def test_absurd_future_extension_is_ignored(self):
        started_at = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        deadline = poller._policy_deadline_ms(
            "123",
            "1",
            started_at,
            120,
            {
                "123-1": {
                    "until_ms": int(
                        (datetime.now(timezone.utc) + timedelta(days=365)).timestamp()
                        * 1000
                    )
                }
            },
        )

        expected_base = poller._iso_to_utc_ms(started_at) + 120 * 60 * 1000
        self.assertEqual(deadline, expected_base)

    def test_bounded_extension_is_used(self):
        started_at = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        until_ms = int((datetime.now(timezone.utc) + timedelta(hours=4)).timestamp() * 1000)

        self.assertEqual(
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": until_ms}},
            ),
            until_ms,
        )


if __name__ == "__main__":
    unittest.main()
