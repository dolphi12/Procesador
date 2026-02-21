"""Parsers de fecha/hora y registros de checadas.

Este módulo es la **fuente de verdad** para el parseo (migrado del legacy).

Principios:
- Tolerante a datos sucios (None/NaN/formatos mixtos).
- `parse_time()` acepta HH:MM o HH:MM:SS y normaliza a minutos.
- `parse_registro()` extrae todas las horas encontradas, preserva orden de aparición
  y deduplica horas exactas (HH:MM).
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import List, Optional

import pandas as pd
from .config import AppConfig


__all__ = ["parse_time", "parse_date", "parse_registro"]


_TIME_RE = re.compile(r"(?P<h>\d{1,2}):(?P<m>\d{1,2})(?::(?P<s>\d{1,2}))?")


def parse_time(value: object) -> Optional[time]:
    """Parsea una hora a `datetime.time`.

    Args:
        value: String u objeto convertible a string (p.ej. "09:10" o "09:10:12").

    Returns:
        `time` con segundos a 0, o `None` si no es parseable/está fuera de rango.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = _TIME_RE.search(s)
    if not m:
        return None

    h = int(m.group("h"))
    mi = int(m.group("m"))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        return None
    return time(hour=h, minute=mi, second=0)


def parse_date(value: object, cfg: AppConfig | None = None):
    """Parsea fechas tolerando formatos sucios.
    - Si detecta ISO (YYYY-MM-DD), usa format fijo y no depende de dayfirst.
    - Para otros formatos, usa cfg.dayfirst (MX típico dd/mm).
    Retorna datetime.date o None.
    """
    import pandas as pd
    import re

    cfg = cfg or AppConfig()
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "nat", "none"}:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d", dayfirst=False)
        else:
            dt = pd.to_datetime(s, errors="coerce", dayfirst=bool(cfg.dayfirst))
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None

def parse_registro(registro_raw: object) -> List[time]:
    """Extrae checadas de una celda/registro.

    Args:
        registro_raw: Celda de Excel (string, NaN, None, etc.). Se buscan patrones de hora.

    Returns:
        Lista de `time` en orden de aparición, deduplicada por HH:MM.
    """
    s = "" if registro_raw is None else str(registro_raw)

    found: List[time] = []
    seen = set()

    for m in _TIME_RE.finditer(s):
        t = parse_time(m.group(0))
        if t is None:
            continue
        key = (t.hour, t.minute)
        if key in seen:
            continue
        seen.add(key)
        found.append(t)

    return found
