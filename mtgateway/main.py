"""Entrypoint for the MTProto Session Gateway."""

import uvicorn

from .app import create_app
from .config import Config


def main() -> None:
    cfg = Config()
    uvicorn.run(create_app(cfg), host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()
