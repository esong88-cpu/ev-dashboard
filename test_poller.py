import unittest
from datetime import datetime, timedelta, timezone

from poller import (
    enrich_policy_complete_since,
    enrich_stations_with_port_sessions,
    _iso_to_utc_ms,
)


class PollerMetadataValidationTests(unittest.TestCase):
    def test_invalid_public_reset_timestamp_does_not_poison_session(self):
        started_at = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        reset_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        stations = {
            "123": {
                "device_id": 123,
                "ports": {"1": "Charging"},
            }
        }
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started_at}},
            }
        }
        reset_map = {
            "123-1": {
                "at": "not-a-date",
                "at_ms": reset_ms,
            }
        }

        enrich_stations_with_port_sessions(prev_root, stations, reset_map)

        session_started = stations["123"]["port_sessions"]["1"]["started_at"]
        self.assertNotEqual(session_started, "not-a-date")
        self.assertGreater(_iso_to_utc_ms(session_started), _iso_to_utc_ms(started_at))

        # Regression: the next enrichment step used to crash when reset.at was copied
        # into started_at and parsed as an ISO timestamp.
        enrich_policy_complete_since(prev_root, stations, {}, 120)

    def test_absurd_future_public_reset_is_ignored(self):
        started_at = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        future_reset_ms = int(
            (datetime.now(timezone.utc) + timedelta(days=365)).timestamp() * 1000
        )
        stations = {
            "123": {
                "device_id": 123,
                "ports": {"1": "Charging"},
            }
        }
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": started_at}},
            }
        }

        enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": future_reset_ms}},
        )

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], started_at)

    def test_invalid_existing_session_timestamp_does_not_crash_policy_enrichment(self):
        stations = {
            "123": {
                "device_id": 123,
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "not-a-date"}},
            }
        }

        enrich_policy_complete_since({}, stations, {}, 120)

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "not-a-date",
        )
        self.assertNotIn("policy_complete_since", stations["123"]["port_sessions"]["1"])


if __name__ == "__main__":
    unittest.main()
