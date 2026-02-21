"""
Shared helpers (sanitization, backups, formatting).
"""
from __future__ import annotations
import pandas as pd

import os
import re
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import hashlib
import hmac
import secrets

def safe_str(v: object) -> str:
    """Convierte a string seguro (None -> '')."""
    return "" if v is None else str(v)


SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._\- ]+")

def sanitize_filename(name: str, max_len: int = 120) -> str:
    """
    Produce a filesystem-safe filename component.
    Prevents path traversal and strips problematic characters.
    """
    name = (name or "").strip().replace("\\", "_").replace("/", "_")
    name = SAFE_NAME_RE.sub("_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_len] if len(name) > max_len else name

def backup_file(path: Path, *, suffix: str = ".bak") -> Optional[Path]:
    """
    Create a timestamped backup before overwriting.
    Returns backup path if created.
    """
    path = Path(path)
    if not path.exists():
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    bak = path.with_suffix(path.suffix + f"{suffix}_{ts}")
    shutil.copy2(path, bak)
    return bak

def harden_permissions(path: Path) -> None:
    """
    Best-effort file permission tightening (0600).
    On Windows it may be ignored.
    """
    try:
        os.chmod(str(path), 0o600)
    except Exception:
        pass


def normalize_id(v: object, width: int = 3) -> str:
    """Normaliza ID y rellena con ceros a la izquierda (compat v18)."""
    s = safe_str(v).strip()
    if not s:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    if s.isdigit() and width > 0:
        s = s.zfill(width)
    return s


def chmod_restringido(path: Path) -> None:
    """Intenta restringir permisos del archivo (POSIX).

    - Archivos: 600
    - Carpetas: 700

    En Windows no hace nada (sin romper).
    """
    try:
        if os.name != "posix":
            return
        if path.is_dir():
            path.chmod(0o700)
        elif path.exists():
            path.chmod(0o600)
    except Exception:
        # No fallar por permisos
        return


def coerce_id_str(v: object, width: int = 3) -> str:
    """Alias de compatibilidad: normaliza ID (ceros a la izquierda)."""
    return normalize_id(v, width=width)


def hhmm_to_minutes(s: str) -> int:
    """Convierte 'HH:MM' a minutos. Soporta vacío/None."""
    if s is None:
        return 0
    s = str(s).strip()
    if not s or s.lower() in {"nan", "none"}:
        return 0
    m = re.match(r"^(\d+):(\d{2})$", s)
    if not m:
        return 0
    return int(m.group(1)) * 60 + int(m.group(2))


def minutes_to_hhmm(total_min: int) -> str:
    if total_min <= 0:
        return "00:00"
    h = total_min // 60
    m = total_min % 60
    return f"{h:02d}:{m:02d}"


def _dia_abrev_es(weekday: int) -> str:
    # 0=Lun ... 6=Dom
    mapa = {0: "Lun", 1: "Mar", 2: "Mié", 3: "Jue", 4: "Vie", 5: "Sáb", 6: "Dom"}
    return mapa.get(int(weekday) % 7, "Dia")


def rango_semana(fecha: pd.Timestamp, week_start_dow: int) -> str:
    dow = fecha.weekday()
    delta = (dow - week_start_dow) % 7
    inicio = fecha - pd.Timedelta(days=delta)
    fin = inicio + pd.Timedelta(days=6)
    return f"{inicio.strftime('%Y-%m-%d')} a {fin.strftime('%Y-%m-%d')}"


def _week_key(d: pd.Timestamp, cfg: AppConfig) -> str:
    """Clave de semana según el inicio configurado (week_start_dow).
    - 0=Lunes (ISO), 2=Miércoles, etc.
    Devuelve una clave estable tipo: YYYY-WK-YYYYMMDD (fecha de inicio de semana).
    """
    try:
        dow = int(getattr(cfg, "week_start_dow", 0) or 0)
    except Exception:
        dow = 0
    dow = dow % 7
    # pandas Timestamp -> datetime.date
    ts = pd.to_datetime(d, errors="coerce")
    if pd.isna(ts):
        return "INVALID-WK"
    dd = ts.date()
    delta = (dd.weekday() - dow) % 7
    ws = dd - timedelta(days=delta)
    return f"{ws.year:04d}-WK-{ws.strftime('%Y%m%d')}"


def _month_key(d: pd.Timestamp) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _norm(s: object) -> str:
    return "" if s is None else str(s).strip().lower()

def _guess_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Devuelve el nombre de la columna del DF que matchea alguno de los candidatos (normalizados)."""
    norm_map = {_norm(c): c for c in df.columns}
    for cand in candidates:
        key = _norm(cand)
        if key in norm_map:

            return norm_map[key]
    return None


def _coerce_id_str(x: object, min_width: int) -> str:
    s = "" if x is None else str(x).strip()
    if not s:
        return ""
    # Excel a veces convierte 003 -> 3. Si es numérico puro, aplica zfill
    if re.fullmatch(r"\d+", s) and len(s) < min_width:
        return s.zfill(min_width)
    return s


def fmt_hhmm(t: 'time | None') -> str:
    """Formatea una hora como HH:MM o vacío."""
    return "" if t is None else t.strftime("%H:%M")


def sha256_file(path: Path) -> str:
    """Devuelve SHA256 hex del archivo (streaming)."""
    path = Path(path)
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def default_app_data_dir(appname: str = "procesador") -> Path:
    """Directorio recomendado para datos/estado (incluye secretos locales).

    - Windows: %APPDATA%\\<appname>
    - Linux/macOS: $XDG_DATA_HOME/<appname> o ~/.local/share/<appname>

    Nota: se crea si no existe.
    """
    appname = sanitize_filename(appname or "procesador", max_len=40) or "procesador"
    if os.name == "nt":
        base = os.getenv("APPDATA") or os.path.expanduser("~")
        p = Path(base) / appname
    else:
        xdg = os.getenv("XDG_DATA_HOME")
        if xdg:
            p = Path(xdg) / appname
        else:
            p = Path.home() / ".local" / "share" / appname
    # Crear directorio; si no se puede (permisos), caer a temp del sistema.
    try:
        p.mkdir(parents=True, exist_ok=True)
        # sanity: intentar escribir un archivo de prueba para asegurar permisos reales
        probe = p / ".__probe__"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except PermissionError:
        p = Path(tempfile.gettempdir()) / appname
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        # fallback ultra conservador
        p = Path(tempfile.gettempdir()) / appname
        p.mkdir(parents=True, exist_ok=True)
    try:
        chmod_restringido(p)
    except Exception:
        pass
    return p


def get_or_create_audit_key(
    script_dir: Path | None = None,
    filename: str = "audit_key.txt",
    *,
    key_dir: Path | None = None,
    appname: str = "procesador",
) -> tuple[str, str]:
    """Obtiene o crea una llave secreta (hex) para firmar auditoría.

    Seguridad:
    - Por defecto, la llave se guarda en un directorio de datos de la app (no junto al código).
    - Permisos best-effort: 0600.

    Retorna: (key_hex, key_id) donde key_id es una huella corta (8 chars).
    """
    # Backward compat: si el caller aún pasa script_dir pero no key_dir, NO guardamos ahí
    # para evitar distribuir secretos con el código. Solo usar script_dir si explícitamente
    # se pasa key_dir=script_dir desde arriba.
    base_dir = Path(key_dir) if key_dir else default_app_data_dir(appname=appname)

    # En algunos entornos (servicios, carpetas bloqueadas) el directorio de appdata puede
    # no ser escribible. En ese caso, caer a un directorio temporal del sistema (no se distribuye).
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        tmp = Path(tempfile.gettempdir()) / appname
        tmp.mkdir(parents=True, exist_ok=True)
        base_dir = tmp

    key_path = base_dir / filename

    def _ensure_key_at(path: Path) -> str:
        """Lee o crea la llave; si no puede por permisos, lanza PermissionError."""
        if path.exists():
            return path.read_text(encoding="utf-8", errors="replace").strip()
        key_hex_local = secrets.token_hex(32)  # 256-bit
        path.write_text(key_hex_local, encoding="utf-8")
        chmod_restringido(path)
        return key_hex_local

    try:
        key_hex = _ensure_key_at(key_path)
    except PermissionError:
        tmp = Path(tempfile.gettempdir()) / appname
        tmp.mkdir(parents=True, exist_ok=True)
        key_path = tmp / filename
        key_hex = _ensure_key_at(key_path)
    # key_id = sha256(key) first 8 chars
    kid = hashlib.sha256(bytes.fromhex(key_hex)).hexdigest()[:8]
    return key_hex, kid

def hmac_sha256_hex(key_hex: str, payload: bytes) -> str:
    """Firma payload con HMAC-SHA256 y devuelve hex."""
    key = bytes.fromhex((key_hex or "").strip())
    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def verify_hmac_sha256_hex(key_hex: str, payload: bytes, signature_hex: str) -> bool:
    """Verifica firma HMAC-SHA256."""
    sig = (signature_hex or "").strip().lower()
    expected = hmac_sha256_hex(key_hex, payload).lower()
    try:
        return hmac.compare_digest(expected, sig)
    except Exception:
        return False
