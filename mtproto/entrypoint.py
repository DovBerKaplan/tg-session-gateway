"""Docker entrypoint shim: apply session persistence, then exec the app.

Usage in Dockerfile:
    COPY mtproto/ /app/mtproto/
    CMD ["python", "-m", "mtproto.entrypoint", "python", "-m", "gateway.main"]

Or as a sitecustomize (automatic on any Python start):
    COPY mtproto/ /usr/local/lib/python3.12/site-packages/mtproto_persist/
    # Python auto-imports sitecustomize.py from site-packages
"""

import runpy
import sys


def main():
    from .patch import apply
    apply()

    # Execute the real application
    if len(sys.argv) < 2:
        print("usage: python -m mtproto.entrypoint <app-module-or-script>")
        sys.exit(1)

    target = sys.argv[1]
    sys.argv = sys.argv[1:]  # shift so the app sees its own argv

    # If it's a file path, run it; if a module name, run with -m semantics
    import os
    if os.path.isfile(target):
        runpy.run_path(target, run_name="__main__")
    else:
        runpy.run_module(target, run_name="__main__")


if __name__ == "__main__":
    main()
