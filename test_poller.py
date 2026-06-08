import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone


def _install_dependency_stubs():
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []

    credentials = types.ModuleType("firebase_admin.credentials")
    credentials.Certificate = lambda value: value

    db = types.ModuleType("firebase_admin.db")
    db.reference = lambda path: None

    firebase_admin.credentials = credentials
    firebase_admin.db = db
    firebase_admin.initialize_app = lambda *args, **kwargs: None

    requests = types.ModuleType("requests")
    requests.codes = types.SimpleNamespace(ok=200)

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = object

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
    sys.modules.setdefault("python_chargepoint", chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_dependency_stubs()
poller = importlib.import_module("poller")


class TimerMetadataHardeningTest(unittest.TestCase):
    def test_poisoned_reset_iso_is_not_written_or_crashing_policy_enrichment(self):
        reset_ms = int((datetime.now(timezone.utc) - timedelta(seconds=5)).timestamp() * 1000)
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_map = {"123-1": {"at_ms": reset_ms, "at": "not an iso timestamp"}}

        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, reset_map)
        self.assertEqual([], cleared)

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(
            datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat(),
            started_at,
        )

        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_far_future_reset_is_ignored(self):
        old_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        reset_ms = int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp() * 1000)

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at_ms": reset_ms, "at": "2099-01-01T00:00:00+00:00"}},
        )

        self.assertEqual(old_start, stations["123"]["port_sessions"]["1"]["started_at"])

    def test_invalid_persisted_started_at_is_replaced(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not an iso timestamp"}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        parsed = datetime.fromisoformat(started_at)
        self.assertIsNotNone(parsed.tzinfo)
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_public_extension_deadlines_cannot_shorten_or_bypass_policy(self):
        started = datetime.now(timezone.utc) - timedelta(hours=3)
        started_at = started.isoformat()
        base = int(started.timestamp() * 1000) + 120 * 60 * 1000
        valid_until = int((datetime.now(timezone.utc) + timedelta(minutes=30)).timestamp() * 1000)

        self.assertEqual(
            base,
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": base - 60 * 1000}},
            ),
        )
        self.assertEqual(
            base,
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp() * 1000)}},
            ),
        )
        self.assertEqual(
            valid_until,
            poller._policy_deadline_ms(
                "123",
                "1",
                started_at,
                120,
                {"123-1": {"until_ms": valid_until}},
            ),
        )


if __name__ == "__main__":
    unittest.main()
