"""Broker credentials come from the systemd credential, whole, or not at all."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import broker_secrets
import shoonya_login

FULL = {
    "SHOONYA_USERID": "FA1234",
    "SHOONYA_PASSWORD": "pw",
    "SHOONYA_TOKEN": "JBSWY3DPEHPK3PXP",
    "SHOONYA_OAUTH_SECRET": "oauth",
    "SHOONYA_VENDOR_CODE": "FA1234_U",
}


class SystemdCredentialTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        clean = {k: v for k, v in os.environ.items()
                 if not k.startswith("SHOONYA_") and k != "CREDENTIALS_DIRECTORY"}
        patcher = mock.patch.dict(os.environ, clean, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _credential(self, payload: str) -> None:
        (self.dir / broker_secrets.CREDENTIAL_NAME).write_text(payload, encoding="utf-8")
        os.environ["CREDENTIALS_DIRECTORY"] = str(self.dir)

    def test_reads_the_decrypted_credential(self) -> None:
        self._credential(json.dumps({**FULL, "SHOONYA_IMEI": "  ", "EXTRA": None}))
        resolved = broker_secrets.resolve_secrets()
        self.assertEqual(resolved.source, "systemd")
        self.assertEqual(dict(resolved.values), FULL)

    def test_outside_the_service_is_a_permanent_error(self) -> None:
        with self.assertRaisesRegex(broker_secrets.SecretsPermanentError, "secret.sh run"):
            broker_secrets.resolve_secrets()

    def test_never_falls_back_to_the_environment(self) -> None:
        os.environ.update(FULL)
        with self.assertRaises(broker_secrets.SecretsPermanentError):
            broker_secrets.resolve_secrets()

    def test_incomplete_or_malformed_is_rejected(self) -> None:
        for payload, pattern in (
            (json.dumps({"SHOONYA_USERID": "x"}), "missing required"),
            ("not json", "not valid JSON"),
            ("[]", "JSON object"),
        ):
            with self.subTest(payload=payload):
                self._credential(payload)
                with self.assertRaisesRegex(broker_secrets.SecretsPermanentError, pattern):
                    broker_secrets.resolve_secrets()

    def test_unloaded_credential_names_the_unit_setting(self) -> None:
        os.environ["CREDENTIALS_DIRECTORY"] = str(self.dir)
        with self.assertRaisesRegex(broker_secrets.SecretsPermanentError, "LoadCredentialEncrypted"):
            broker_secrets.resolve_secrets()

    def test_env_mode_takes_the_environment_whole(self) -> None:
        os.environ["SHOONYA_SECRETS_SOURCE"] = "env"
        os.environ.update(FULL)
        self.assertEqual(broker_secrets.resolve_secrets().source, "env")
        del os.environ["SHOONYA_PASSWORD"]
        with self.assertRaisesRegex(broker_secrets.SecretsPermanentError, "SHOONYA_PASSWORD"):
            broker_secrets.resolve_secrets()

    def test_unknown_mode_stays_on_the_encrypted_credential(self) -> None:
        os.environ["SHOONYA_SECRETS_SOURCE"] = "aws"
        self.assertEqual(broker_secrets.source_mode(), "systemd")

    def test_load_credentials_end_to_end(self) -> None:
        self._credential(json.dumps(FULL))
        creds = shoonya_login.load_credentials()
        self.assertEqual(creds.userid, "FA1234")
        self.assertEqual(creds.client_id, "FA1234_U")
        self.assertIn("encrypted credential", creds.source)

        del os.environ["CREDENTIALS_DIRECTORY"]
        with self.assertRaises(shoonya_login.PermanentLoginError):
            shoonya_login.load_credentials()


if __name__ == "__main__":
    unittest.main()
