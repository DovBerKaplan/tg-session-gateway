"""Transparent session persistence patch for Pyrogram / pyrofork.

Usage (Dockerfile or entrypoint, BEFORE importing the application):

    from mtproto import apply
    apply()          # patches Client.__init__ transparently

    # then import/start your app as usual — zero changes

What it does:
    Client(name, ..., in_memory=True)
        ↓ intercepted
    Client(name, ..., in_memory=False, workdir=SESSION_DIR)
        ↓ Pyrogram's native file-based session handling
    {SESSION_DIR}/{name}.session exists?
        YES → load auth_key → skip authorize() → connect in ~1s
        NO  → authenticate once → Pyrogram saves the session file
              every future start loads it → never re-auths again

The patch is idempotent (safe to call multiple times) and opt-out via
PYROGRAM_PERSIST_SESSIONS=0.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger("mtproto.persist")

SESSION_DIR = os.getenv("PYROGRAM_SESSION_DIR", "/data/sessions")
ENABLED = os.getenv("PYROGRAM_PERSIST_SESSIONS", "1").lower() not in ("0", "false", "no")

_applied = False


def apply() -> bool:
    """Patch Pyrogram's Client.__init__ for transparent persistence.

    Returns True if the patch was applied (or already was), False if
    disabled by env or Pyrogram not installed."""
    global _applied
    if _applied:
        return True
    if not ENABLED:
        log.info("session persistence disabled (PYROGRAM_PERSIST_SESSIONS=0)")
        return False

    try:
        import pyrogram.client
    except ImportError:
        try:
            import pyrofork.client as pyrogram_client
        except ImportError:
            log.warning("Pyrogram/pyrofork not installed — patch skipped")
            return False
        pyrogram = type("m", (), {"client": pyrogram_client})
    else:
        pyrogram_client = pyrogram.client

    os.makedirs(SESSION_DIR, exist_ok=True)

    _orig_init = pyrogram_client.Client.__init__

    def _persistent_init(self, name, *args, **kwargs):
        if kwargs.get("in_memory"):
            session_file = os.path.join(SESSION_DIR, f"{name}.session")
            if os.path.exists(session_file):
                # Saved session exists → use file-based (skip re-auth)
                kwargs["in_memory"] = False
                kwargs["workdir"] = SESSION_DIR
                log.info("session '%s': using persisted file", name)
            else:
                # First boot: let Pyrogram authenticate in memory,
                # but also save the session for future boots.
                kwargs["in_memory"] = False
                kwargs["workdir"] = SESSION_DIR
                log.info("session '%s': first boot — will persist to %s",
                         name, SESSION_DIR)
        _orig_init(self, name, *args, **kwargs)

    pyrogram_client.Client.__init__ = _persistent_init
    _applied = True
    log.info("Pyrogram session persistence ACTIVE (dir=%s)", SESSION_DIR)
    return True


def status() -> dict:
    """Introspection: is the patch active, what sessions exist."""
    sessions = []
    if os.path.isdir(SESSION_DIR):
        for f in sorted(os.listdir(SESSION_DIR)):
            if f.endswith(".session"):
                path = os.path.join(SESSION_DIR, f)
                sessions.append({
                    "name": f[:-8],
                    "size_bytes": os.path.getsize(path),
                    "modified": os.path.getmtime(path),
                })
    return {"applied": _applied, "enabled": ENABLED, "dir": SESSION_DIR,
            "sessions": sessions}
