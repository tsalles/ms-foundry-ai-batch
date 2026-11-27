import json
import unittest

from foundry_batch.config import ResourceConfig, Settings, is_loopback_url
from foundry_batch.errors import ConfigurationError
from tests.support import make_env, make_settings


class SettingsTests(unittest.TestCase):
    def test_vault_verification_defaults_and_demo_opt_out(self) -> None:
        for environment, flag, expected in (
            ("test", "", False),
            ("test", "true", True),
            ("poc", "", True),
            ("poc", "false", False),
            ("consumer", "", True),
            ("consumer", "false", True),
        ):
            with self.subTest(environment=environment, flag=flag):
                settings = make_settings(
                    BATCH_ENVIRONMENT=environment, BATCH_VAULT_SYNC_REQUIRED=flag
                )
                self.assertEqual(settings.requires_vault_verification, expected)

    def test_defaults_match_the_terraform_contract(self) -> None:
        s = make_settings()
        self.assertEqual(s.incoming_queue, "batch-events")
        self.assertEqual(s.relay_queue, "relay-dispatch")
        self.assertEqual(s.table_jobs, "batchjobs")
        self.assertEqual(s.relay_signing_secret_name, "relay-signing-test")
        self.assertEqual(s.status_call_floor_seconds, 60)
        self.assertEqual(s.max_body_bytes, 32 * 1024)
        self.assertEqual([r.key for r in s.resources], ["eus2", "swc"])

    def test_resource_lookup_and_naming_rules(self) -> None:
        s = make_settings()
        resource = s.resource("eus2")
        assert resource is not None
        self.assertEqual(
            resource.signing_secret_names("test"), ("whsec-test-eus2-a", "whsec-test-eus2-b")
        )
        self.assertEqual(resource.endpoint_name("test", "b"), "batch-test-eus2-b")
        self.assertIsNone(s.resource("missing"))

    def test_endpoint_gets_trailing_slash(self) -> None:
        env = make_env(
            BATCH_RESOURCES_JSON=json.dumps(
                [{"key": "eus2", "endpoint": "https://x.openai.azure.com", "region": "eastus2"}]
            )
        )
        self.assertEqual(
            Settings.from_env(env).resources[0].endpoint, "https://x.openai.azure.com/"
        )

    def test_demo_downstream_url_defaults_to_own_host(self) -> None:
        self.assertEqual(
            make_settings().effective_downstream_url(), "https://func.example.net/api/demo/events"
        )
        s = make_settings(
            BATCH_DOWNSTREAM_MODE="http", BATCH_DOWNSTREAM_URL="https://batch.example/x"
        )
        self.assertEqual(s.effective_downstream_url(), "https://batch.example/x")
        self.assertEqual(make_settings(WEBSITE_HOSTNAME="").effective_downstream_url(), "")

    def test_all_problems_reported_together_without_values(self) -> None:
        env = make_env(
            BATCH_ENVIRONMENT="Bad Env",
            BATCH_MAX_BODY_BYTES="999999",
            BATCH_TIMESTAMP_TOLERANCE_SECONDS="abc",
            BATCH_DOWNSTREAM_MODE="http",
            BATCH_UNKNOWN_JOB_POLICY="trust",
            BATCH_INCOMING_QUEUE="Bad_Queue",
            BATCH_TABLE_JOBS="1bad",
            BATCH_RECEIVER_RATE_PER_SECOND="x",
        )
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(env)
        message = str(ctx.exception)
        for fragment in (
            "BATCH_ENVIRONMENT",
            "BATCH_MAX_BODY_BYTES",
            "BATCH_TIMESTAMP_TOLERANCE",
            "BATCH_DOWNSTREAM_URL is required",
            "BATCH_UNKNOWN_JOB_POLICY",
            "BATCH_INCOMING_QUEUE",
            "BATCH_TABLE_JOBS",
            "BATCH_RECEIVER_RATE_PER_SECOND",
        ):
            self.assertIn(fragment, message)

    def test_status_floor_cannot_go_below_sixty_seconds(self) -> None:
        with self.assertRaises(ConfigurationError):
            make_settings(BATCH_STATUS_CALL_FLOOR_SECONDS="30")
        with self.assertRaises(ConfigurationError):
            make_settings(BATCH_RECEIVER_RATE_PER_SECOND="0")

    def test_resource_json_validation(self) -> None:
        cases = {
            "": "is required",
            "not json": "not valid JSON",
            "{}": "non-empty JSON array",
            "[1]": "must be an object",
            json.dumps([{"key": "EUS", "endpoint": "http://x", "region": ""}]): "key must match",
            json.dumps(
                [
                    {"key": "a1", "endpoint": "https://x", "region": "r"},
                    {"key": "a1", "endpoint": "https://y", "region": "r"},
                ]
            ): "duplicate key",
            json.dumps(
                [{"key": "a1", "endpoint": "https://x", "region": "r", "deployments": "x"}]
            ): "deployments must be a list",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw), self.assertRaises(ConfigurationError) as ctx:
                Settings.from_env(make_env(BATCH_RESOURCES_JSON=raw))
            self.assertIn(expected, str(ctx.exception))

    def test_http_downstream_must_be_https(self) -> None:
        with self.assertRaises(ConfigurationError):
            make_settings(BATCH_DOWNSTREAM_URL="http://plain.example")

    def test_range_bounds_are_inclusive(self) -> None:
        cases = (
            ("BATCH_TIMESTAMP_TOLERANCE_SECONDS", "timestamp_tolerance_seconds", 30, 900, 1),
            ("BATCH_RECEIVER_RATE_PER_SECOND", "receiver_rate_per_second", 1.0, 10_000.0, 1.0),
            ("BATCH_OUTBOX_BASE_DELAY_SECONDS", "outbox_base_delay_seconds", 0.1, 600.0, 0.05),
        )
        for name, attribute, low, high, step in cases:
            with self.subTest(name=name):
                for accepted in (low, high):
                    settings = make_settings(**{name: str(accepted)})
                    self.assertEqual(getattr(settings, attribute), accepted)
                for refused in (low - step, high + step):
                    with self.assertRaises(ConfigurationError) as ctx:
                        make_settings(**{name: str(refused)})
                    self.assertIn(f"{name} must be between {low} and {high}", str(ctx.exception))

    def test_missing_environment_is_reported_as_required(self) -> None:
        env = make_env()
        del env["BATCH_ENVIRONMENT"]
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(env)
        self.assertIn("BATCH_ENVIRONMENT is required", str(ctx.exception))
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(make_env(BATCH_ENVIRONMENT="   "))
        self.assertIn("BATCH_ENVIRONMENT is required", str(ctx.exception))

    def test_unknown_downstream_mode_is_refused(self) -> None:
        with self.assertRaises(ConfigurationError) as ctx:
            make_settings(BATCH_DOWNSTREAM_MODE="kafka")
        self.assertIn("BATCH_DOWNSTREAM_MODE must be 'demo' or 'http'", str(ctx.exception))

    def test_resource_shape_problems(self) -> None:
        cases = {
            json.dumps([{"key": "a1", "endpoint": "https://x"}]): "[0].region is required",
            json.dumps({"key": "a1"}): "must be a non-empty JSON array",
            json.dumps("text"): "must be a non-empty JSON array",
            "[]": "must be a non-empty JSON array",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw), self.assertRaises(ConfigurationError) as ctx:
                Settings.from_env(make_env(BATCH_RESOURCES_JSON=raw))
            self.assertIn(expected, str(ctx.exception))

    def test_vault_sync_required_flag(self) -> None:
        self.assertFalse(make_settings().vault_sync_required)
        for raw, expected in (
            ("true", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("false", False),
            ("0", False),
            ("no", False),
            ("", False),
        ):
            with self.subTest(raw=raw):
                settings = make_settings(BATCH_VAULT_SYNC_REQUIRED=raw)
                self.assertIs(settings.vault_sync_required, expected)
        with self.assertRaises(ConfigurationError) as ctx:
            make_settings(BATCH_VAULT_SYNC_REQUIRED="ture")
        self.assertIn("BATCH_VAULT_SYNC_REQUIRED must be true or false", str(ctx.exception))

    def test_resource_config_is_hashable_value_object(self) -> None:
        a = ResourceConfig("k1", "https://x/", "r", ("d",))
        self.assertEqual(a, ResourceConfig("k1", "https://x/", "r", ("d",)))


class LoopbackUrlTests(unittest.TestCase):
    """S9: 'loopback' is decided on the parsed host, never on a string prefix."""

    def test_loopback_urls(self) -> None:
        for url in (
            "http://127.0.0.1:9/r/eus2/",
            "http://localhost:7071/api/demo/events",
            "http://[::1]:8080/",
            "http://localhost/",
            "https://127.0.0.1:8443/x",
            "HTTP://127.0.0.1:80/",
        ):
            with self.subTest(url=url):
                self.assertTrue(is_loopback_url(url))

    def test_lookalikes_are_not_loopback(self) -> None:
        for url in (
            "http://127.0.0.1:1@evil.example/",
            "http://localhost:80@evil.example/x",
            "http://user:pw@127.0.0.1:1/",
            "http://127.0.0.1:80.evil.example/",
            "http://localhost.evil.com:80/",
            "http://127.0.0.1.nip.io:80/",
            "http://0.0.0.0:80/",
            "http://127.1:80/",
            "http://127.0.0.1:99999/",
            "http://127.0.0.1:1\\@evil.example/",
            "http://127.0.0.1:1 @evil.example/",
            "ftp://127.0.0.1/",
            "",
        ):
            with self.subTest(url=url):
                self.assertFalse(is_loopback_url(url))

    def test_settings_refuse_userinfo_endpoints_and_downstream(self) -> None:
        evil = "http://127.0.0.1:1" + "@" + "evil.example/"
        resources = json.dumps([{"key": "eus2", "endpoint": evil, "region": "r"}])
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(
                make_env(
                    BATCH_ENVIRONMENT="dev",
                    WEBSITE_HOSTNAME="",
                    BATCH_RESOURCES_JSON=resources,
                    BATCH_FOUNDRY_AUTH="emulator",
                    BATCH_STORAGE_BACKEND="memory",
                    BATCH_SECRETS_BACKEND="memory",
                )
            )
        self.assertIn("[0].endpoint must use https", str(ctx.exception))
        self.assertIn("never send a placeholder token", str(ctx.exception))
        with self.assertRaises(ConfigurationError) as ctx:
            Settings.from_env(
                make_env(
                    BATCH_ENVIRONMENT="dev",
                    WEBSITE_HOSTNAME="",
                    BATCH_DOWNSTREAM_MODE="http",
                    BATCH_DOWNSTREAM_URL=evil,
                )
            )
        self.assertIn("BATCH_DOWNSTREAM_URL must use https", str(ctx.exception))
        ok = Settings.from_env(
            make_env(
                BATCH_ENVIRONMENT="dev",
                WEBSITE_HOSTNAME="",
                BATCH_DOWNSTREAM_URL="http://[::1]:7071/api/demo/events",
            )
        )
        self.assertEqual(ok.downstream_url, "http://[::1]:7071/api/demo/events")


if __name__ == "__main__":
    unittest.main()
