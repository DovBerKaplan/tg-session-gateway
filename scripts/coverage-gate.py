#!/usr/bin/env python3
"""Coverage ratchet: per-package floors that may only move UP.

Floors below 80 for the two planes reflect the gotd-facing internals
(Connect/media/upload) that fake-driven tests would cover dishonestly;
raising them means real integration tests, not mocks.

Usage: coverage-gate.py <coverprofile> [floors-file]
Exits 1 when any package drops below its floor.
"""
import sys

DEFAULT_FLOORS = """\
# package (module-relative) floor
internal/botapi 72.0
internal/mtproto 62.0
internal/store 58.0
internal/rateguard 57.0
internal/migrate 68.0
internal/cryptoutil 84.0
internal/health 84.0
internal/lock 85.0
internal/mtproto/pyformat 83.0
pkg/client 79.0
"""

def main() -> int:
    profile = sys.argv[1]
    floors_path = sys.argv[2] if len(sys.argv) > 2 else None
    floors = {}
    for line in open(floors_path or [], encoding="utf-8") if floors_path else DEFAULT_FLOORS.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pkg, floor = line.rsplit(" ", 1)
        floors[pkg] = float(floor)

    totals: dict[str, list[int]] = {}
    for line in open(profile, encoding="utf-8"):
        if line.startswith("mode:"):
            continue
        # <file>:<start>,<end> <numstmt> <count>
        loc, stmts, count = line.split(" ")
        pkg = loc.removeprefix("github.com/DovBerKaplan/tg-session-gateway/").rsplit("/", 1)[0]
        acc = totals.setdefault(pkg, [0, 0])
        acc[0] += int(stmts)
        if int(count) > 0:
            acc[1] += int(stmts)

    fail = 0
    for pkg, floor in sorted(floors.items()):
        acc = totals.get(pkg)
        if acc is None:
            print(f"MISS  {pkg} (no profile blocks)")
            fail = 1
            continue
        pct = 100.0 * acc[1] / acc[0] if acc[0] else 0.0
        mark = "OK  " if pct >= floor else "FAIL"
        if pct < floor:
            fail = 1
        print(f"{mark}  {pkg} {pct:.1f}% (floor {floor}%)")
    return fail

if __name__ == "__main__":
    sys.exit(main())
