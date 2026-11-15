"""Uvicorn entry point for the control plane."""

from __future__ import annotations

import logging

import uvicorn

from .api import create_app
from .config import Config
from .logging import setup_logging

log = logging.getLogger("agentmesh.server")


def run_server(cfg: Config) -> None:
    setup_logging(cfg.server.log_level, "control-plane", json_logs=cfg.server.json_logs)
    app = create_app(cfg)
    log.info("listening on http://%s:%s", cfg.server.host, cfg.server.port)
    uvicorn.run(
        app,
        host=cfg.server.host,
        port=cfg.server.port,
        log_config=None,
        access_log=False,
        timeout_keep_alive=30,
        lifespan="on",
    )
