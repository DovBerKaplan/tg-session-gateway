"""Container preflight (gateway/preflight.py) — the Postgres-style hard
requirements: fail fast on a missing secret or a non-volume /data."""

import os

import pytest
from cryptography.fernet import Fernet

from gateway.preflight import genkey, preflight


class TestPreflight:
    def test_missing_fernet_key_fails_fast(self):
        with pytest.raises(SystemExit) as e:
            preflight(None, "/data")
        assert "GW_FERNET_KEY is required" in e.value.code
        assert "refuses to generate secrets" in e.value.code

    def test_empty_fernet_key_fails_fast(self):
        with pytest.raises(SystemExit):
            preflight("", "/data")

    def test_data_dir_not_a_mount_fails(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GW_ALLOW_EPHEMERAL_DATA", raising=False)
        with pytest.raises(SystemExit) as e:
            preflight("some-key", str(tmp_path))  # tmp_path is not a mount
        assert "must be a mounted volume" in e.value.code

    def test_allow_ephemeral_data_opts_in(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GW_ALLOW_EPHEMERAL_DATA", "1")
        preflight("some-key", str(tmp_path))  # no SystemExit

    def test_mounted_data_dir_passes(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GW_ALLOW_EPHEMERAL_DATA", raising=False)
        monkeypatch.setattr(os.path, "ismount", lambda p: True)
        preflight("some-key", str(tmp_path))


class TestGenkey:
    def test_prints_a_valid_fernet_key(self, capsys):
        genkey()
        key = capsys.readouterr().out.strip()
        # a roundtrip proves the printed key is a real Fernet key
        token = Fernet(key.encode()).encrypt(b"roundtrip")
        assert Fernet(key.encode()).decrypt(token) == b"roundtrip"
