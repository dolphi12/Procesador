"""Utilidades para consolidar (merge) entradas diarias del colector.

Uso típico:
- Operación diaria: procesar 1 archivo por día.
- Cierre semanal/mensual: consolidar varios archivos diarios en un solo Excel y procesar ese consolidado.

Este módulo NO toca reglas de negocio; solo concatena y limpia entradas.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd


REQUIRED_COLS = ["ID", "Nombre", "Fecha", "Registro"]


@dataclass(frozen=True)
class MergeReport:
    files_read: int
    rows_in: int
    rows_out: int
    duplicates_dropped: int
    output_path: Path


def _read_input_file(path: Path) -> pd.DataFrame:
    """Lee un archivo .xlsx/.xls/.csv como DataFrame.

    - Para Excel toma la primera hoja.
    - Normaliza nombres de columnas (strip).
    """
    suffix = path.suffix.lower()
    if suffix in {".csv"}:
        df = pd.read_csv(path)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, sheet_name=0)
    else:
        raise ValueError(f"Formato no soportado para merge: {path.name}")

    df.columns = [str(c).strip() for c in df.columns]
    return df


def _coerce_fecha_to_iso(df: pd.DataFrame) -> pd.DataFrame:
    if "Fecha" not in df.columns:
        return df
    s = df["Fecha"]
    # Si viene datetime, convertir a YYYY-MM-DD
    try:
        s2 = pd.to_datetime(s, errors="coerce")
        # si al menos una fecha parsea, usar eso
        if s2.notna().any():
            df["Fecha"] = s2.dt.date.astype(str)
            return df
    except Exception:
        pass
    # fallback: cast a str y extraer YYYY-MM-DD si existe
    df["Fecha"] = s.astype(str).str.strip()
    return df


def _ensure_required(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura que existan columnas mínimas; si faltan, las crea vacías."""
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = ""
    # 'Número de pases de la tarjeta' es opcional (default 1)
    if "Número de pases de la tarjeta" not in df.columns:
        df["Número de pases de la tarjeta"] = 1
    return df


def merge_inputs(
    inputs: Sequence[Path],
    output_path: Path,
    *,
    dedupe: bool = True,
    sort: bool = True,
    keep_extra_cols: bool = False,
) -> MergeReport:
    """Consolida múltiples archivos de entrada en un solo Excel.

    Parámetros
    ----------
    inputs:
        Lista de archivos (xlsx/xls/csv) a consolidar.
    output_path:
        Ruta del Excel resultante.
    dedupe:
        Si True, elimina duplicados exactos por (ID, Nombre, Fecha, Registro).
    sort:
        Si True, ordena por Fecha ascendente y luego por ID/Nombre.
    keep_extra_cols:
        Si False, exporta solo columnas esperadas por el procesador.

    Retorna:
        MergeReport con métricas básicas.
    """
    if not inputs:
        raise ValueError("No hay archivos de entrada para merge.")

    dfs: List[pd.DataFrame] = []
    rows_in = 0
    for p in inputs:
        df = _read_input_file(p)
        rows_in += int(len(df))
        df = _ensure_required(df)
        df = _coerce_fecha_to_iso(df)

        # normaliza tipo texto
        df["ID"] = df["ID"].astype(str).str.strip()
        df["Nombre"] = df["Nombre"].astype(str).str.strip()
        df["Registro"] = df["Registro"].astype(str).str.strip()

        # si ID viene 'nan', vaciar
        df.loc[df["ID"].str.lower().eq("nan"), "ID"] = ""
        df.loc[df["Nombre"].str.lower().eq("nan"), "Nombre"] = ""
        df.loc[df["Registro"].str.lower().eq("nan"), "Registro"] = ""
        df.loc[df["Fecha"].str.lower().eq("nan"), "Fecha"] = ""

        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)

    dup_dropped = 0
    if dedupe:
        before = len(out)
        out = out.drop_duplicates(subset=["ID", "Nombre", "Fecha", "Registro"], keep="first")
        dup_dropped = before - len(out)

    if sort:
        # ordenar: Fecha -> ID (si numérico) -> Nombre
        def _id_sort_key(x: str) -> Tuple[int, str]:
            xs = str(x).strip()
            if xs.isdigit():
                return (0, xs.zfill(6))
            return (1, xs)

        if "Fecha" in out.columns:
            out["_id_key__"] = out["ID"].map(_id_sort_key)
            out = out.sort_values(by=["Fecha", "_id_key__", "Nombre"], ascending=[True, True, True])
            out = out.drop(columns=["_id_key__"], errors="ignore")
        else:
            out = out.sort_values(by=["ID", "Nombre"], ascending=[True, True])

    if not keep_extra_cols:
        cols = ["ID", "Nombre", "Fecha", "Número de pases de la tarjeta", "Registro"]
        out = out[[c for c in cols if c in out.columns]]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_excel(output_path, index=False)

    return MergeReport(
        files_read=len(inputs),
        rows_in=rows_in,
        rows_out=int(len(out)),
        duplicates_dropped=int(dup_dropped),
        output_path=output_path,
    )


def collect_inputs(
    input_dir: Path,
    *,
    pattern: str = "*.xlsx",
    recursive: bool = False,
) -> List[Path]:
    """Recolecta archivos en un directorio según patrón."""
    if not input_dir.exists():
        raise FileNotFoundError(str(input_dir))
    if recursive:
        files = sorted(input_dir.rglob(pattern))
    else:
        files = sorted(input_dir.glob(pattern))
    files = [p for p in files if p.is_file()]
    return files
