"""
Central logging configuration for the attendance processor.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

DEFAULT_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def setup_logging(level: str = "INFO", log_file: Optional[Path] = None) -> logging.Logger:
    """
    Configure root logging once.
    - level: DEBUG/INFO/WARNING/ERROR
    - log_file: if provided, also logs to file (UTF-8)
    """
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(lvl)

    # Avoid duplicate handlers when re-importing.
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        sh = logging.StreamHandler()
        sh.setLevel(lvl)
        sh.setFormatter(logging.Formatter(DEFAULT_FMT))
        root.addHandler(sh)

    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        if not any(getattr(h, "baseFilename", None) == str(log_file) for h in root.handlers):
            fh = logging.FileHandler(str(log_file), encoding="utf-8")
            fh.setLevel(lvl)
            fh.setFormatter(logging.Formatter(DEFAULT_FMT))
            root.addHandler(fh)

        # Best-effort permissions hardening (Windows may ignore).
        try:
            os.chmod(str(log_file), 0o600)
        except Exception:
            pass

    return logging.getLogger("procesador")


def log_exception(msg: str, *, extra: dict | None = None, level: int = logging.WARNING) -> None:
    """Loggea una excepción con contexto sin romper el flujo."""
    if extra:
        try:
            ctx = " ".join([f"{k}={v!r}" for k, v in extra.items()])
            msg = f"{msg} | {ctx}"
        except Exception:
            pass
    logging.getLogger("procesador").log(level, msg, exc_info=True)

