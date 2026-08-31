import sys
import types
import unittest
from datetime import datetime, timezone


def _install_dependency_stubs() -> None:
    firebase_admin = types.ModuleType("firebase_admin")
    firebase_admin._apps = []
    firebase_admin.initialize_app = lambda *args, **kwargs: None
    firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
    firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)

    chargepoint = types.ModuleType("python_chargepoint")
    chargepoint.ChargePoint = type("ChargePoint", (), {})
    exceptions = types.ModuleType("python_chargepoint.exceptions")
    exceptions.ChargePointCommunicationException = type(
        "ChargePointCommunicationException", (Exception,), {}
    )
    exceptions.ChargePointLoginError = type("ChargePointLoginError", (Exception,), {})

    sys.modules.setdefault("firebase_admin", firebase_admin)
    sys.modules.setdefault("firebase_admin.credentials", firebase_admin.credentials)
    sys.modules.setdefault("firebase_admin.db", firebase_admin.db)
    sys.modules.setdefault("python_chargepoint", chargepoint)
    sys.modules.setdefault("python_chargepoint.exceptions", exceptions)


_install_dependency_stubs()

import poller  # noqa: E402


class ResetMetadataTests(unittest.TestCase):
    def test_reset_uses_validated_at_ms_instead_of_untrusted_at_string(self) -> None:
        old_start_ms = 1_700_000_000_000
        reset_ms = old_start_ms + 60_000
        old_start = datetime.fromtimestamp(old_start_ms / 1000, tz=timezone.utc).isoformat()

        started_at = poller._reset_started_at_from_metadata(
            {"at_ms": reset_ms, "at": "not-an-iso-date"},
            old_start,
            now_ms=reset_ms,
        )

        self.assertEqual(
            started_at,
            datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat(),
        )

    def test_reset_rejects_non_finite_and_far_future_values(self) -> None:
        now_ms = 1_700_000_000_000
        old_start = datetime.fromtimestamp((now_ms - 60_000) / 1000, tz=timezone.utc).isoformat()

        self.assertIsNone(
            poller._reset_started_at_from_metadata(
                {"at_ms": float("inf")},
                old_start,
                now_ms=now_ms,
            )
        )
        self.assertIsNone(
            poller._reset_started_at_from_metadata(
                {"at_ms": now_ms + poller.RESET_FUTURE_SKEW_MS + 1},
                old_start,
                now_ms=now_ms,
            )
        )

    def test_malformed_public_reset_does_not_crash_policy_enrichment(self) -> None:
        old_start_ms = 1_700_000_000_000
        reset_ms = old_start_ms + 60_000
        old_start = datetime.fromtimestamp(old_start_ms / 1000, tz=timezone.utc).isoformat()
        expected_start = datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc).isoformat()
        prev_root = {
            "100": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": old_start}},
            }
        }
        stations = {"100": {"ports": {"1": "Charging"}}}

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"100-1": {"at_ms": reset_ms, "at": "not-an-iso-date"}},
        )
        poller.enrich_policy_complete_since(prev_root, stations, {}, 120)

        self.assertEqual(stations["100"]["port_sessions"]["1"]["started_at"], expected_start)


if __name__ == "__main__":
    unittest.main()
