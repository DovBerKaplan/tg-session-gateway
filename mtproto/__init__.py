"""Transparent MTProto session persistence for Pyrogram.

ZERO application changes: the patch intercepts Client.__init__ and
converts in_memory=True to file-based sessions on a persistent volume.
Deploys stop calling ImportBotAuthorization → no FloodWait.
"""

from .patch import apply

__all__ = ["apply"]
