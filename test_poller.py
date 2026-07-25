import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import poller


class NormalizePortStatusTests(unittest.TestCase):
    def test_unavailable_is_not_misclassified_as_available(self):
        for status, status_v2 in (
            ("UNAVAILABLE", ""),
            ("unavailable", ""),
            ("", "UNAVAILABLE"),
            ("FAULT", ""),
            ("OFFLINE", ""),
        ):
            with self.subTest(status=status, status_v2=status_v2):
                self.assertEqual(
                    poller._normalize_port_status(status, status_v2),
                    "Unavailable",
                )

    def test_available_and_charging_still_map_correctly(self):
        self.assertEqual(poller._normalize_port_status("AVAILABLE", ""), "Available")
        self.assertEqual(poller._normalize_port_status("CHARGING", ""), "Charging")
        self.assertEqual(poller._normalize_port_status("IN_USE", ""), "Charging")
        self.assertEqual(poller._normalize_port_status("fully_charged", ""), "Complete")

    def test_not_charging_and_inactive_avoid_substring_false_positives(self):
        self.assertEqual(poller._normalize_port_status("NOT_CHARGING", ""), "Available")
        self.assertEqual(poller._normalize_port_status("INACTIVE", ""), "Inactive")


class HomeChargerStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_not_charging_while_plugged_in_is_complete(self):
        client = SimpleNamespace(
            get_home_charger_status=AsyncMock(
                return_value=SimpleNamespace(
                    charging_status="NOT_CHARGING",
                    is_plugged_in=True,
                    brand="ChargePoint",
                    model="Home Flex",
                )
            )
        )

        payload = await poller.fetch_home_charger_status(client, 99)

        self.assertEqual(payload["ports"]["1"], "Complete")

    async def test_not_charging_while_unplugged_is_available(self):
        client = SimpleNamespace(
            get_home_charger_status=AsyncMock(
                return_value=SimpleNamespace(
                    charging_status="NOT_CHARGING",
                    is_plugged_in=False,
                    brand="ChargePoint",
                    model="Home Flex",
                )
            )
        )

        payload = await poller.fetch_home_charger_status(client, 99)

        self.assertEqual(payload["ports"]["1"], "Available")


class OccupancyClearingTests(unittest.TestCase):
    def test_unavailable_port_does_not_clear_like_available(self):
        """Unavailable must stay occupied-bucket-adjacent: not 'available'."""
        self.assertEqual(
            poller._session_occupancy_bucket(
                poller._normalize_port_status("UNAVAILABLE", "")
            ),
            "occupied",
        )
        self.assertEqual(
            poller._session_occupancy_bucket(
                poller._normalize_port_status("AVAILABLE", "")
            ),
            "available",
        )

    def test_unavailable_status_preserves_session_and_extensions(self):
        prev_root = {
            "123": {
                "ports": {"1": "Charging"},
                "port_sessions": {"1": {"started_at": "2026-07-25T10:00:00+00:00"}},
            }
        }
        stations = {
            "123": {
                "ports": {
                    "1": poller._normalize_port_status("UNAVAILABLE", ""),
                }
            }
        }

        cleared = poller.enrich_stations_with_port_sessions(prev_root, stations, {})

        self.assertEqual(cleared, [])
        self.assertEqual(
            stations["123"]["port_sessions"]["1"]["started_at"],
            "2026-07-25T10:00:00+00:00",
        )


if __name__ == "__main__":
    unittest.main()
