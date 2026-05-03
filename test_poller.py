from datetime import datetime, timezone
from unittest import TestCase, main
from unittest.mock import patch

import poller


class PollerSessionTests(TestCase):
    def test_enrich_sessions_ignores_future_public_reset(self):
        prev_started = "2026-05-03T10:00:00+00:00"
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": prev_started}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}
        far_future_ms = int(
            datetime(2099, 1, 1, tzinfo=timezone.utc).timestamp() * 1000
        )

        poller.enrich_stations_with_port_sessions(
            prev_root,
            stations,
            {"123-1": {"at": "2099-01-01T00:00:00+00:00", "at_ms": far_future_ms}},
        )

        self.assertEqual(stations["123"]["port_sessions"]["1"]["started_at"], prev_started)

    def test_enrich_sessions_applies_recent_public_reset(self):
        prev_started = "2026-05-03T10:00:00+00:00"
        reset_ms = int(
            datetime(2026, 5, 3, 10, 30, tzinfo=timezone.utc).timestamp() * 1000
        )
        now_ms = int(
            datetime(2026, 5, 3, 10, 31, tzinfo=timezone.utc).timestamp() * 1000
        )
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": prev_started}},
            }
        }
        stations = {"123": {"ports": {"1": "Charging"}}}

        with patch.object(poller, "_current_utc_ms", return_value=now_ms):
            poller.enrich_stations_with_port_sessions(
                prev_root,
                stations,
                {"123-1": {"at": "ignored-client-string", "at_ms": reset_ms}},
            )

        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-05-03T10:30:00+00:00",
        )

    def test_available_port_clears_extension_and_reset_metadata(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-05-03T10:00:00+00:00"}},
            }
        }
        stations = {"123": {"ports": {"1": "Available"}}}

        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual(cleared, ["123-1"])
        self.assertNotIn("port_sessions", stations["123"])


if __name__ == "__main__":
    main()
