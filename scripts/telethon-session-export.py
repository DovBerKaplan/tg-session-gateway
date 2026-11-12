#!/usr/bin/env python3
"""Export a legacy Telethon .session (SQLite) as a Telethon StringSession.

Stdlib only; run with `python3 -I`. The output is a secret equivalent to
the account login: it is written to --out with mode 0600 (never printed),
and an existing file is never overwritten. Feed it to the gateway
(`POST /v1/admin/sessions/{alias}/import` with {"telethon_string": ...}),
then delete it.

`--out -` writes to stdout for a pipe straight into the gateway import
endpoint, so the value never lands in a file; it
is refused when stdout is a terminal (the key must never reach a screen).

`--refuse-key-of <live.session>` (repeatable) refuses to export when the
session carries the same auth key as a session another process is using:
that would be one key live in two processes (AUTH_KEY_DUPLICATED). Keys are
compared in memory only.

Exporting the live scanner's OWN session means stopping it first: one auth
key may be live in exactly one process, otherwise Telegram returns
AUTH_KEY_DUPLICATED to both.
"""
import argparse
import base64
import ipaddress
import os
import sqlite3
import struct
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("session", help="path to <name>.session (opened read-only)")
    ap.add_argument("--out", required=True,
                    help="output file (created 0600, must not exist), or - for a pipe")
    ap.add_argument("--refuse-key-of", action="append", default=[], metavar="SESSION",
                    help="refuse when this .session (e.g. the live scanner's) has the same auth key")
    args = ap.parse_args()

    if args.out == "-" and sys.stdout.isatty():
        print("refusing to write the session to a terminal; pipe it into session-import",
              file=sys.stderr)
        return 1

    rows = read_rows(args.session)
    if len(rows) != 1:
        print(f"expected exactly 1 row in sessions, found {len(rows)}", file=sys.stderr)
        return 1
    dc_id, server, port, key = rows[0]
    if not key or len(key) != 256:
        print("session has no 256-byte auth key (not logged in?)", file=sys.stderr)
        return 1
    for other in args.refuse_key_of:
        if any(k == key for _, _, _, k in read_rows(other)):
            print(f"refusing: {args.session} has the same auth key as {other} "
                  "(one key live in two processes => AUTH_KEY_DUPLICATED)", file=sys.stderr)
            return 1
    ip = ipaddress.ip_address(server).packed
    packed = struct.pack(f">B{len(ip)}sH256s", dc_id, ip, port, key)
    value = "1" + base64.urlsafe_b64encode(packed).decode("ascii")

    if args.out == "-":
        sys.stdout.write(value + "\n")
        sys.stdout.flush()
        print(f"piped string session for DC {dc_id}", file=sys.stderr)
        return 0
    fd = os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(value + "\n")
    print(f"wrote string session for DC {dc_id} to {args.out} (mode 0600)")
    return 0


def read_rows(path: str):
    """All (dc_id, server_address, port, auth_key) rows, opened read-only."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return con.execute(
            "SELECT dc_id, server_address, port, auth_key FROM sessions").fetchall()
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
