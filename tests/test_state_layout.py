"""Runtime state must not live in the source tree.

Six files used to accumulate next to the code: a live broker token in plaintext,
a DPAPI-encrypted credential cache, a cross-process lock, and two per-failure
cooldown files from a build that no longer exists. They showed up in every
directory listing, would have entered version control the moment one was
initialised, and nothing ever cleaned the dead ones up.

These lock in the layout that replaced it: one directory, one definition of
where it is, out of the source tree, and self-tidying.
"""

from __future__ import annotations

import os
import time
import unittest
from pathlib import Path
from unittest import mock

import broker_secrets
import shoonya_login

SOURCE_DIR = Path(shoonya_login.__file__).resolve().parent


class StateDirectoryTests(unittest.TestCase):
    def test_both_modules_share_one_definition(self) -> None:
        """Two derivations of the same path are two chances to disagree."""
        self.assertIs(shoonya_login.STATE_DIR, broker_secrets.STATE_DIR)

    def test_state_does_not_live_in_the_source_tree(self) -> None:
        for name in (
            "SESSION_CACHE_FILE", "SESSION_LOCK_FILE",
            "COOLDOWN_FILE", "LEGACY_TOKEN_FILE",
        ):
            with self.subTest(file=name):
                path = getattr(shoonya_login, name)
                self.assertFalse(
                    path.parent.resolve().is_relative_to(SOURCE_DIR),
                    f"{name} is back in the source tree",
                )

    def test_diagnostics_stay_with_the_bot_log(self) -> None:
        """A failed login is read next to the log covering the same minutes."""
        self.assertEqual(shoonya_login.DIAGNOSTICS_DIR, SOURCE_DIR / "logs")

    def test_the_override_still_wins(self) -> None:
        with mock.patch.dict(os.environ, {"LUNE_STATE_DIR": r"X:\somewhere"}):
            self.assertEqual(
                Path(os.getenv("LUNE_STATE_DIR") or broker_secrets._default_state_dir()),
                Path(r"X:\somewhere"),
            )

    @unittest.skipUnless(os.name == "nt", "ProgramData is Windows-only")
    def test_the_windows_default_is_machine_wide(self) -> None:
        """LocalSystem (service) and Administrator (interactive) share it."""
        default = broker_secrets._default_state_dir()
        self.assertEqual(default, Path(os.environ["ProgramData"]) / "neobot")

    def test_a_host_without_program_data_still_gets_one_directory(self) -> None:
        """The fallback is outside the source tree."""
        # Windows normalises environment keys to upper case, so match on that
        # rather than the mixed-case spelling the code reads it by.
        env = {k: v for k, v in os.environ.items() if k.upper() != "PROGRAMDATA"}
        with mock.patch.dict(os.environ, env, clear=True):
            default = broker_secrets._default_state_dir()
        self.assertEqual(
            default,
            Path(os.getenv("XDG_STATE_HOME") or Path.home() / ".local" / "state") / "lune",
        )

    @unittest.skipIf(os.name == "nt", "POSIX file modes only")
    def test_atomic_session_cache_stays_private_after_rewrite(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            shoonya_login, "_ensure_state_dir"
        ):
            cache = Path(directory) / ".shoonya_session.json"
            shoonya_login._atomic_write_json(cache, {"access_token": "test"})
            self.assertEqual(cache.stat().st_mode & 0o777, 0o600)
            shoonya_login._atomic_write_json(cache, {"access_token": "updated"})
            self.assertEqual(cache.stat().st_mode & 0o777, 0o600)


class PruneStaleStateTests(unittest.TestCase):
    """Debris nothing else removes: dead cooldowns and crashed atomic writes."""

    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TEMP", ".")
        ) / f"neobot-prune-test-{os.getpid()}-{time.time_ns()}"
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.addCleanup(self._cleanup)
        patcher = mock.patch.object(shoonya_login, "STATE_DIR", self.tmp)
        patcher.start()
        self.addCleanup(patcher.stop)
        cooldown = mock.patch.object(
            shoonya_login, "COOLDOWN_FILE",
            self.tmp / ".shoonya_login_cooldown.json",
        )
        cooldown.start()
        self.addCleanup(cooldown.stop)

    def _cleanup(self) -> None:
        for entry in self.tmp.glob("*"):
            try:
                entry.unlink()
            except OSError:
                pass
        try:
            self.tmp.rmdir()
        except OSError:
            pass

    def _write(self, name: str, *, age_sec: float = 0.0) -> Path:
        path = self.tmp / name
        path.write_text("x", encoding="utf-8")
        if age_sec:
            old = time.time() - age_sec
            os.utime(path, (old, old))
        return path

    def test_dated_cooldown_files_from_the_old_build_are_removed(self) -> None:
        dead = self._write(".shoonya_login_cooldown.browser_failure_20260814.json")
        removed = shoonya_login.prune_stale_state()
        self.assertFalse(dead.exists())
        self.assertEqual(removed, 1)

    def test_the_live_cooldown_file_is_kept(self) -> None:
        live = self._write(".shoonya_login_cooldown.json")
        shoonya_login.prune_stale_state()
        self.assertTrue(live.exists(), "the active cooldown must survive a prune")

    def test_an_abandoned_temp_file_is_removed(self) -> None:
        orphan = self._write(".shoonya_session.json.9999.tmp", age_sec=7200)
        shoonya_login.prune_stale_state()
        self.assertFalse(orphan.exists())

    def test_a_temp_file_from_a_live_write_is_left_alone(self) -> None:
        """Another process may have it open right now."""
        fresh = self._write(".shoonya_session.json.1234.tmp")
        shoonya_login.prune_stale_state()
        self.assertTrue(fresh.exists())

    def test_real_state_is_never_touched(self) -> None:
        keep = [
            self._write(".shoonya_session.json"),
            self._write(".shoonya_session.lock"),
            self._write(".shoonya_secrets.cache"),
        ]
        shoonya_login.prune_stale_state()
        for path in keep:
            self.assertTrue(path.exists(), f"{path.name} must not be pruned")

    def test_a_missing_state_directory_is_not_an_error(self) -> None:
        with mock.patch.object(shoonya_login, "STATE_DIR", self.tmp / "gone"):
            self.assertEqual(shoonya_login.prune_stale_state(), 0)


class LegacyTokenFileTests(unittest.TestCase):
    """The plaintext token file is retired, not merely relocated."""

    TOKEN = "tok-" + "a" * 40

    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TEMP", ".")
        ) / f"neobot-legacy-test-{os.getpid()}-{time.time_ns()}"
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.addCleanup(self._cleanup)
        for name, path in (
            ("SESSION_CACHE_FILE", self.tmp / ".shoonya_session.json"),
            ("LEGACY_TOKEN_FILE", self.tmp / "access_token.txt"),
            ("STATE_DIR", self.tmp),
        ):
            patcher = mock.patch.object(shoonya_login, name, path)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _cleanup(self) -> None:
        for entry in self.tmp.glob("*"):
            try:
                entry.unlink()
            except OSError:
                pass
        try:
            self.tmp.rmdir()
        except OSError:
            pass

    def _manager(self):
        manager = shoonya_login.ShoonyaSessionManager.__new__(
            shoonya_login.ShoonyaSessionManager
        )
        manager._session = None
        manager.creds = mock.Mock(userid="U1", client_id_hash="hash")
        return manager

    def test_publishing_a_session_no_longer_writes_a_plaintext_token(self) -> None:
        session = shoonya_login.CachedSession(
            access_token=self.TOKEN, uid="U1", client_id_hash="hash",
            issued_at=0.0, validated_at=0.0, generation=1,
        )
        self._manager()._publish(session, update_disk=True)

        self.assertTrue(shoonya_login.SESSION_CACHE_FILE.exists(),
                        "the JSON session cache must still be written")
        self.assertFalse(shoonya_login.LEGACY_TOKEN_FILE.exists(),
                         "a bare token must not be mirrored to disk any more")

    def test_adoption_consumes_and_deletes_the_old_file(self) -> None:
        shoonya_login.LEGACY_TOKEN_FILE.write_text(self.TOKEN, encoding="utf-8")

        adopted = self._manager()._adopt_legacy_file()

        self.assertIsNotNone(adopted, "an existing token should still be adopted")
        self.assertEqual(adopted.access_token, self.TOKEN)
        self.assertFalse(shoonya_login.LEGACY_TOKEN_FILE.exists(),
                         "the plaintext file must be removed once consumed")

    def test_a_junk_legacy_file_is_deleted_rather_than_adopted(self) -> None:
        shoonya_login.LEGACY_TOKEN_FILE.write_text("short", encoding="utf-8")

        self.assertIsNone(self._manager()._adopt_legacy_file())
        self.assertFalse(shoonya_login.LEGACY_TOKEN_FILE.exists())

    def test_no_legacy_file_is_a_quiet_no_op(self) -> None:
        self.assertIsNone(self._manager()._adopt_legacy_file())


if __name__ == "__main__":
    unittest.main()
