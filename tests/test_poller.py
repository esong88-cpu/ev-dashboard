import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path


def _install_external_stubs() -> None:
    try:
        import firebase_admin  # noqa: F401
    except ModuleNotFoundError:
        firebase_admin = types.ModuleType("firebase_admin")
        firebase_admin._apps = []
        firebase_admin.credentials = types.SimpleNamespace(Certificate=lambda value: value)
        firebase_admin.db = types.SimpleNamespace(reference=lambda path: None)
        firebase_admin.initialize_app = lambda *args, **kwargs: None
        sys.modules["firebase_admin"] = firebase_admin

    try:
        import python_chargepoint  # noqa: F401
        import python_chargepoint.exceptions  # noqa: F401
    except ModuleNotFoundError:
        chargepoint = types.ModuleType("python_chargepoint")
        chargepoint.ChargePoint = object

        exceptions = types.ModuleType("python_chargepoint.exceptions")

        class ChargePointCommunicationException(Exception):
            def __init__(self, *args, **kwargs):
                super().__init__(*args)

        class ChargePointLoginError(Exception):
            pass

        exceptions.ChargePointCommunicationException = ChargePointCommunicationException
        exceptions.ChargePointLoginError = ChargePointLoginError
        sys.modules["python_chargepoint"] = chargepoint
        sys.modules["python_chargepoint.exceptions"] = exceptions


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
_install_external_stubs()

import poller  # noqa: E402


class PortSessionEnrichmentTest(unittest.TestCase):
    def test_applied_manual_reset_marks_slot_metadata_for_cleanup(self) -> None:
        reset_at = datetime(2026, 5, 7, 11, 0, tzinfo=timezone.utc)
        reset_ms = int(reset_at.timestamp() * 1000)
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {
                    "1": {"started_at": "2026-05-07T08:00:00+00:00"},
                },
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        keys_to_clear = poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "not a trusted timestamp", "at_ms": reset_ms}},
        )

        self.assertEqual(keys_to_clear, ["123-1"])
        started_at = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertEqual(started_at, reset_at.isoformat())

        ext_map = {"123-1": {"until_ms": reset_ms + 8 * 60 * 60 * 1000}}
        for key in keys_to_clear:
            ext_map.pop(key, None)

        self.assertEqual(
            poller._policy_deadline_ms("123", "1", started_at, 120, ext_map),
            reset_ms + 120 * 60 * 1000,
        )

    def test_invalid_iso_timestamp_does_not_crash_poller_helpers(self) -> None:
        self.assertEqual(poller._iso_to_utc_ms("not-a-date"), 0)


if __name__ == "__main__":
    unittest.main()
