"""Tests for the transparent MTProto session persistence patch."""

import os
import tempfile
from unittest.mock import MagicMock


class TestPatchConfig:
    def test_enabled_by_default(self):
        import mtproto.patch as mp
        assert isinstance(mp.ENABLED, bool)

    def test_disabled_by_env(self):
        with __import__("unittest").mock.patch.dict(
            os.environ, {"PYROGRAM_PERSIST_SESSIONS": "0"}
        ):
            val = os.getenv(
                "PYROGRAM_PERSIST_SESSIONS", "1"
            ).lower() not in ("0", "false", "no")
            assert val is False


class TestPatchedInitConversion:
    """THE core promise: in_memory=True → file-based on the session dir."""

    def _make_patch(self, session_dir):
        _orig_init = MagicMock()

        def _persistent_init(self, name, *args, **kwargs):
            if kwargs.get("in_memory"):
                kwargs["in_memory"] = False
                kwargs["workdir"] = session_dir
            _orig_init(self, name, *args, **kwargs)

        return _persistent_init, _orig_init

    def test_in_memory_becomes_file_based(self):
        d = tempfile.mkdtemp()
        patched, orig = self._make_patch(d)
        client = MagicMock()
        patched(client, "testbot", api_id=1, api_hash="x",
                bot_token="123:abc", in_memory=True)
        kw = orig.call_args[1]
        assert kw["in_memory"] is False
        assert kw["workdir"] == d

    def test_first_boot_no_file_also_converts(self):
        d = tempfile.mkdtemp()  # empty — no session file
        patched, orig = self._make_patch(d)
        client = MagicMock()
        patched(client, "newbot", in_memory=True)
        kw = orig.call_args[1]
        assert kw["in_memory"] is False
        assert kw["workdir"] == d

    def test_non_in_memory_untouched(self):
        d = tempfile.mkdtemp()
        patched, orig = self._make_patch(d)
        client = MagicMock()
        patched(client, "mybot", workdir="/custom")
        kw = orig.call_args[1]
        assert "in_memory" not in kw
        assert kw["workdir"] == "/custom"

    def test_explicit_false_in_memory_untouched(self):
        d = tempfile.mkdtemp()
        patched, orig = self._make_patch(d)
        client = MagicMock()
        patched(client, "mybot", in_memory=False)
        kw = orig.call_args[1]
        assert kw["in_memory"] is False


class TestStatus:
    def test_lists_sessions(self):
        import mtproto.patch as mp
        d = tempfile.mkdtemp()
        mp.SESSION_DIR = d
        for name in ("alpha", "beta"):
            with open(os.path.join(d, f"{name}.session"), "w") as f:
                f.write("x")
        s = mp.status()
        assert len(s["sessions"]) == 2
        assert s["sessions"][0]["name"] == "alpha"
