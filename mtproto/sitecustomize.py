"""Auto-applied on any Python start (place in site-packages/).

This file is imported automatically by Python's site module before any
application code runs. It applies the Pyrogram session persistence
patch with zero changes to the application.

Placement in Dockerfile:
    COPY mtproto/sitecustomize.py /usr/local/lib/python3.12/site-packages/
"""

try:
    from mtproto.patch import apply
    apply()
except ImportError:
    pass  # mtproto module not available — run unpatched
except Exception:
    pass  # never break the app because the patch failed
