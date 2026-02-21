"""Aplicación de grupos e IDGRUPO (v18-compatible)."""

from __future__ import annotations

import re

from typing import Dict, Optional, Tuple
from pathlib import Path

import pandas as pd

from .utils import normalize_id
from .utils import _coerce_id_str
from .config import AppConfig


def _safe_input(prompt: str, default: str = "") -> str:
    """Input seguro para modo interactivo; devuelve default en EOF."""
    try:
        v = input(prompt)
    except EOFError:
        return default
    v = (v or "").strip()
    return v if v else default


def _grupo_sort_key(emp_id: str, cfg: AppConfig) -> Tuple[int, str]:
    g = cfg.empleado_a_grupo.get(emp_id, "")
    try:
        idx = cfg.grupos_orden.index(g)
    except ValueError:
        idx = 9999
    return idx, emp_id


def aplicar_grupos_y_idgrupo(df: pd.DataFrame, cols: Dict[str, str], cfg: AppConfig, *, permitir_interactivo: bool) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelve (df_procesado, df_idgrupo).
    - df_procesado: ID original, ordenado por grupo
    - df_idgrupo: IDGRUPO, ordenado por grupo
    """
    col_id = cols["id"]
    col_nombre = cols["nombre"]

    # Actualizar meta (nombre) y estado default (activo) para IDs detectados
    try:
        meta = getattr(cfg, "empleado_meta", None)
        status = getattr(cfg, "empleado_status", None)
        if isinstance(meta, dict):
            # usa el primer nombre visto por ID
            if col_nombre in df.columns:
                for _id, _nm in df[[col_id, col_nombre]].dropna().drop_duplicates().itertuples(index=False, name=None):
                    k = str(_id).strip()
                    if not k:
                        continue
                    meta.setdefault(k, {})
                    if _nm and not meta[k].get("nombre"):
                        meta[k]["nombre"] = str(_nm).strip()
            else:
                for _id in df[[col_id]].dropna().drop_duplicates().itertuples(index=False, name=None):
                    k = str(_id).strip()
                    if k:
                        meta.setdefault(k, {})
        if isinstance(status, dict):
            for _id in df[[col_id]].dropna().drop_duplicates().itertuples(index=False, name=None):
                k = str(_id).strip()
                if not k or k.startswith("NOMBRE::"):
                    continue
                status.setdefault(k, {"activo": True})
    except Exception:
        pass

    # Asegurar asignación: si falta grupo/idgrupo y está permitido, pedirlo
    ids = df[col_id].fillna("").astype(str).tolist()
    for emp in ids:
        if not emp:
            continue
        # Caso SIN ID (clave interna NOMBRE::...):
        # - No forzar captura interactiva
        # - Asignar grupo interno SIN_ID solo para ordenar al final
        # - Mantener IDGRUPO vacío para que NO afecte reportes por IDGRUPO
        if str(emp).startswith("NOMBRE::"):
            if emp not in cfg.empleado_a_grupo:
                cfg.empleado_a_grupo[emp] = "SIN_ID"
            # no asignar IDGRUPO
            if emp not in cfg.empleado_a_idgrupo:
                cfg.empleado_a_idgrupo[emp] = ""
            continue
        if emp not in cfg.empleado_a_grupo and permitir_interactivo:
            print(f"\nEmpleado nuevo detectado: ID={emp}.")
            print("Grupos disponibles (en orden): " + " | ".join(cfg.grupos_orden))
            # Selección de grupo con validación (evita teclear algo accidental)
            while True:
                g_in = _safe_input("Asigna grupo (teclea exactamente como aparece) [Enter = primer grupo]: ", "").strip()
                if not g_in:
                    cfg.empleado_a_grupo[emp] = cfg.grupos_orden[0]
                    break
                if g_in in cfg.grupos_orden:
                    cfg.empleado_a_grupo[emp] = g_in
                    break
                resp = _safe_input(f"El grupo '{g_in}' no existe. ¿Deseas crearlo? (S/N): ", "N").strip().upper()
                if resp == "S":
                    cfg.grupos_orden.append(g_in)

                    if g_in not in cfg.grupos_meta:
                        cfg.grupos_meta[g_in] = {"prefijo": g_in}
                    cfg.empleado_a_grupo[emp] = g_in
                    break
        if emp not in cfg.empleado_a_idgrupo and permitir_interactivo:
            # Si es un empleado SIN ID (clave NOMBRE::), no generamos IDGRUPO automático.
            if str(emp).startswith("NOMBRE::"):
                cfg.empleado_a_idgrupo[emp] = ""
            else:
                g = cfg.empleado_a_grupo.get(emp, cfg.grupos_orden[0])
                pref = cfg.prefijo_de_grupo(g)
                sugerido = f"{pref}-{emp}"
                nuevo = _safe_input(f"IDGRUPO para ID={emp} (Enter para usar '{sugerido}'): ", "").strip()
                cfg.empleado_a_idgrupo[emp] = nuevo if nuevo else sugerido
    # Construir columnas calculadas (sin mostrar grupo/prefijo según requerimiento)
    df2 = df.copy()
    df2["_grp_idx"] = df2[col_id].map(lambda x: _grupo_sort_key(str(x), cfg)[0])
    df2["_grp_idgrupo"] = df2[col_id].map(lambda x: cfg.empleado_a_idgrupo.get(str(x), ""))
    # Orden por grupo y luego por ID (estable)
    df2 = df2.sort_values(by=["_grp_idx", col_id], kind="stable").reset_index(drop=True)
    # df_idgrupo: reemplaza ID por IDGRUPO visible
    df_idgrupo = df2.copy()
    df_idgrupo.insert(0, "IDGRUPO", df_idgrupo["_grp_idgrupo"])
    # En archivo _IDGRUPO solo se conserva la columna IDGRUPO (se elimina el ID original)
    df_idgrupo = df_idgrupo.drop(columns=[col_id], errors="ignore")
    # quitar columnas internas
    df_idgrupo = df_idgrupo.drop(columns=["_grp_idx", "_grp_idgrupo"], errors="ignore")
    # df_procesado: mantiene ID original, pero mismo orden de df2
    df_procesado = df2.drop(columns=["_grp_idx", "_grp_idgrupo"], errors="ignore")
    # Guardar config si hubo cambios
    return df_procesado, df_idgrupo


def build_idgrupo_label(idgrupo_code: str, emp_id: object, cfg: AppConfig) -> str:
    """Construye etiqueta IDGRUPO = <IDGRUPO>-<ID_EMPLEADO> (p.ej. 000-03, F-115, FT-124).

    Reglas:
    - Si `emp_id` es numérico, se convierte a string sin decimales y se aplica padding mínimo
      `cfg.idgrupo_emp_min_width` (por defecto 2) SOLO si el id tiene menos dígitos.
    - Si `emp_id` ya es string con ceros, se conserva.
    - Si `idgrupo_code` está vacío, retorna ''.

    Args:
        idgrupo_code: Código del grupo (ej. '000', 'F', 'FT').
        emp_id: ID del empleado.
        cfg: Config.

    Returns:
        Etiqueta combinada o ''.
    """
    code = (idgrupo_code or "").strip()
    if not code:
        return ""

    emp = normalize_id(emp_id, width=0)  # no fuerza padding global
    if emp.isdigit() and cfg.idgrupo_emp_min_width > 0 and len(emp) < cfg.idgrupo_emp_min_width:
        emp = emp.zfill(cfg.idgrupo_emp_min_width)

    return f"{code}{cfg.idgrupo_sep}{emp}"


from typing import Callable

import pandas as pd


def transform_sheet_procesado(df: pd.DataFrame) -> pd.DataFrame:
    """En _PROCESADO todas las hojas conservan ID original y NO llevan IDGRUPO."""
    return df.drop(columns=["IDGRUPO"], errors="ignore")


def transform_sheet_idgrupo(
    df: pd.DataFrame,
    cfg: AppConfig,
    idgrupo_of: Callable[[object], str],
) -> pd.DataFrame:
    """En _IDGRUPO todas las hojas llevan IDGRUPO combinado y NO llevan ID.

    Si la hoja no tiene columna 'ID', se deja igual.
    """
    if "ID" not in df.columns:
        return df

    df2 = df.copy()
    if "IDGRUPO" in df2.columns:
        df2 = df2.drop(columns=["IDGRUPO"], errors="ignore")

    df2.insert(
        0,
        "IDGRUPO",
        df2["ID"].map(lambda x: build_idgrupo_label(idgrupo_of(x), x, cfg)),
    )
    df2 = df2.drop(columns=["ID"], errors="ignore")
    return df2


def _norm(s: object) -> str:
    return "" if s is None else str(s).strip().lower()

def _is_digits(s: object) -> bool:
    return bool(re.fullmatch(r"\d+", str(s).strip())) if s is not None else False


def make_emp_key(id_val: object, nombre_val: object, cfg_id_width: int = 3) -> Tuple[str, str]:
    """Crea una clave interna estable por empleado y el ID visible.
    Caso especial SIN ID:
      - En el export, a veces ID == Nombre (ej. 'María') y no es numérico.
      - Internamente usamos clave: 'NOMBRE::' para no perder trazabilidad.
      - En reportes visibles, el ID se deja en blanco (para no confundir).
    Retorna: (emp_key, id_display)
    """
    id_raw = _coerce_id_str(id_val, cfg_id_width)
    nombre_raw = "" if nombre_val is None else str(nombre_val).strip()
    if id_raw and nombre_raw and (_norm(id_raw) == _norm(nombre_raw)) and (not _is_digits(id_raw)):
        return f"NOMBRE::{_norm(nombre_raw)}", ""  # ID visible vacío
    # Si el ID no es numérico y parece nombre (contiene letras), úsalo como clave tipo NOMBRE::
    # Esto ayuda a que correcciones/plantilla coincidan si se captura 'María' como ID.
    if id_raw and (not _is_digits(id_raw)) and re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", id_raw):
        return f"NOMBRE::{_norm(id_raw)}", ""
    return id_raw, id_raw


def apply_id_display(df: Optional[pd.DataFrame], id_col: str = "ID") -> Optional[pd.DataFrame]:
    """Devuelve copia con ID visible en blanco para claves NOMBRE::"""
    if df is None:
        return None
    if id_col not in df.columns:
        return df
    out = df.copy()
    try:
        out[id_col] = out[id_col].astype(str).map(lambda x: "" if str(x).startswith("NOMBRE::") else str(x))
    except Exception:
        pass
    return out



def sort_df_by_group(df: pd.DataFrame, cfg: AppConfig, *, id_col: str = 'ID', idgrupo_col: str = 'IDGRUPO') -> pd.DataFrame:
    """Ordena un DF por orden de grupo configurado.

    - Si existe `id_col`, usa cfg.empleado_a_grupo[id] para obtener el índice.
    - Si no existe `id_col` pero existe `idgrupo_col`, toma el prefijo antes del separador.
    """
    if df is None or len(df)==0:
        return df
    df2 = df.copy()
    if id_col in df2.columns:
        df2['_grp_idx'] = df2[id_col].astype(str).map(lambda x: _grupo_sort_key(str(x), cfg)[0])
        df2 = df2.sort_values(by=['_grp_idx', id_col], kind='stable').drop(columns=['_grp_idx']).reset_index(drop=True)
        return df2
    if idgrupo_col in df2.columns:
        sep = getattr(cfg, 'idgrupo_sep', '-') or '-'
        def _idx(v: object) -> int:
            s = '' if v is None else str(v).strip()
            code = s.split(sep,1)[0] if sep in s else s
            try:
                return cfg.grupos_orden.index(code)
            except Exception:
                return 9999
        df2['_grp_idx'] = df2[idgrupo_col].map(_idx)
        df2 = df2.sort_values(by=['_grp_idx', idgrupo_col], kind='stable').drop(columns=['_grp_idx']).reset_index(drop=True)
        return df2
    return df
