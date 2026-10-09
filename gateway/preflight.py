"""Container preflight — the Postgres-style hard requirements.

nginx runs bare because it holds no secrets and no state. This gateway
holds both, so it follows the postgres contract instead: refuse to
start when the operator skipped a secret or the data volume, instead of
inventing identity inside the container layer.

- GW_FERNET_KEY: a key generated in-container dies with the container;
  after a restart nothing decrypts. Generate one OUTSIDE (genkey
  subcommand, or openssl) and pass it in.
- /data must be a mounted volume: sessions, offsets, queue and audit
  live there. Container-local storage silently loses them on restart.
  GW_ALLOW_EPHERMERAL_DATA=1 opts in for throwaway demos.
"""

from __future__ import annotations

import os
import sys

_GENKEY_HINT = (
    "Generate one outside the container:\n"
    "  docker run --rm <image> genkey\n"
    "or: openssl rand -base64 32 | tr '+/' '-_'"
)


def genkey() -> None:
    """Print a fresh Fernet key and exit — the documented way to create
    GW_FERNET_KEY without a local python+cryptography install."""
    from cryptography.fernet import Fernet

    print(Fernet.generate_key().decode())


def preflight(fernet_key: str | None, data_dir: str) -> None:
    if not fernet_key:
        sys.exit(
            "GW_FERNET_KEY is required (encrypts stored bot tokens).\n"
            f"{_GENKEY_HINT}\n"
            "The gateway refuses to generate secrets inside the container:\n"
            "a key written to the container layer is lost on restart and\n"
            "every stored token becomes undecryptable."
        )
    if not os.path.ismount(data_dir) and os.environ.get("GW_ALLOW_EPHEMERAL_DATA") != "1":
        sys.exit(
            f"{data_dir} must be a mounted volume (-v <name>:{data_dir}).\n"
            "Sessions, offsets, the update queue and the audit log live there;\n"
            "container-local storage silently loses them on restart.\n"
            "Set GW_ALLOW_EPHEMERAL_DATA=1 to run anyway (throwaway demos only)."
        )
