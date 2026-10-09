"""Entrypoint for the MTProto Session Gateway."""

import sys

import uvicorn

from gateway.preflight import genkey, preflight

from .app import create_app
from .config import Config


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "genkey":
        genkey()
        return
    cfg = Config()
    cfg.validate()
    preflight(cfg.fernet_key, cfg.data_dir)
    uvicorn.run(create_app(cfg), host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()
