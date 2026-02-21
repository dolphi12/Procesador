"""Append-only audit logging helpers (JSONL).

Contract:
- auditoria_cambios.jsonl: one JSON object per line
- best-effort permissions: file 0600, dir 0700
- input sanitization to avoid control characters / huge payloads

This module is intentionally small and dependency-free.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import logging

from .utils import chmod_restringido, harden_permissions

_log = logging.getLogger("procesador.audit")

CONTROL_CHARS = {chr(i) for i in range(0, 32)} - {"\t"}

def _sanitize_text(s: str, max_len: int = 500) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ")
    s = "".join((" " if ch in CONTROL_CHARS else ch) for ch in s)
    s = " ".join(s.split())
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def sanitize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (rec or {}).items():
        kk = _sanitize_text(str(k), max_len=80)
        if isinstance(v, str):
            out[kk] = _sanitize_text(v, max_len=1200)
        elif v is None or isinstance(v, (int, float, bool)):
            out[kk] = v
        else:
            # evita objetos complejos o binarios
            out[kk] = _sanitize_text(str(v), max_len=1200)
    return out


def ensure_dir_secure(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    try:
        chmod_restringido(d)
    except Exception:
        _log.debug("No se pudo endurecer permisos de %s", d, exc_info=True)


def log_change(
    *,
    audit_dir: Path,
    record: Dict[str, Any],
    filename: str = "auditoria_cambios.jsonl",
    rotate_max_bytes: int = 0,
) -> Path:
    """Append a sanitized record to JSONL audit file."""
    audit_dir = Path(audit_dir)
    ensure_dir_secure(audit_dir)
    path = audit_dir / filename

    # rotate best-effort
    try:
        if rotate_max_bytes and path.exists() and path.stat().st_size > int(rotate_max_bytes):
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated = path.with_name(path.stem + f"_{ts}" + path.suffix)
            path.rename(rotated)
            harden_permissions(rotated)
    except Exception:
        _log.debug("No se pudo rotar archivo de auditoría %s", path, exc_info=True)

    rec = sanitize_record(record)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    harden_permissions(path)
    return path


def log_many(
    *,
    audit_dir: Path,
    records: Iterable[Dict[str, Any]],
    filename: str = "auditoria_cambios.jsonl",
    rotate_max_bytes: int = 0,
) -> Optional[Path]:
    last: Optional[Path] = None
    for r in records:
        last = log_change(
            audit_dir=audit_dir,
            record=r,
            filename=filename,
            rotate_max_bytes=rotate_max_bytes,
        )
    return last


def make_sample_entries() -> list[dict[str, Any]]:
    now = datetime.now().isoformat(timespec="seconds")
    return [
        {
            "run_id": "sample_run_001",
            "emp_id": "003",
            "fecha": "2026-01-29",
            "usuario": "RRHH",
            "ts": now,
            "accion": "EDIT",
            "campo": "Salida a comer",
            "antes": "09:29",
            "despues": "09:30",
            "motivo": "Ajuste por diferencia de 1 minuto (validado con supervisor).",
        },
        {
            "run_id": "sample_run_001",
            "emp_id": "003",
            "fecha": "2026-01-29",
            "usuario": "RRHH",
            "ts": now,
            "accion": "INSERT",
            "campo": "Regreso de cenar",
            "antes": "",
            "despues": "21:35",
            "motivo": "Se agregó regreso a cenar (registro faltante en export).",
        },
        {
            "run_id": "sample_run_001",
            "emp_id": "NOMBRE::JUAN PEREZ",
            "fecha": "2026-01-29",
            "usuario": "RRHH",
            "ts": now,
            "accion": "DELETE",
            "campo": "Entrada",
            "antes": "08:01",
            "despues": "",
            "motivo": "Registro duplicado, se conserva el primero.",
        },
    ]


def write_sample_jsonl(path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in make_sample_entries():
            f.write(json.dumps(sanitize_record(rec), ensure_ascii=False) + "\n")
    harden_permissions(path)
    return path
