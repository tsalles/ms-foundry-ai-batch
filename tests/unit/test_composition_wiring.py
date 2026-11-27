"""Composition passes the injected clock to every provider client (17 Nov full review, R8)."""

import unittest
from datetime import UTC, datetime

from foundry_batch.clock import ManualClock
from foundry_batch.composition import adapters_from_settings
from foundry_batch.emulator.local import local_settings
from foundry_batch.foundry.rest_client import FoundryRestClient


class ProviderClientClockTests(unittest.TestCase):
    def test_every_provider_client_measures_retry_after_on_the_injected_clock(self) -> None:
        clock = ManualClock(datetime(2026, 11, 20, 12, 0, tzinfo=UTC))
        adapters = adapters_from_settings(local_settings(("eus2", "swc")), clock=clock)
        for key in ("eus2", "swc"):
            client = adapters.batch_apis.for_resource(key)
            self.assertIsInstance(client, FoundryRestClient)
            self.assertIs(client._clock, clock)


if __name__ == "__main__":
    unittest.main()
