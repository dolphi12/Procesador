"""Construcción de resúmenes (v18-compatible).

- RESUMEN_SEMANAL
- RESUMEN_MENSUAL
- RESUMEN_SEMANAL_VERTICAL
- RESUM_SEM_CHECADAS
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .utils import hhmm_to_minutes, minutes_to_hhmm, rango_semana, _week_key, _month_key, _dia_abrev_es
from .groups import _grupo_sort_key
from .config import AppConfig

def construir_resumen_semanal(df_out: pd.DataFrame, cfg, faltas_semanal: pd.DataFrame = None) -> pd.DataFrame:
    """Resumen semanal HORIZONTAL.

    Columnas:
    - ID, Nombre, Semana
    - 7 columnas por día (según cfg.week_start_dow) con HH:MM trabajadas ese día
    - Dias_presentes, Total horas trabajadas, Total horas extra, Faltas, Rango semana

    Nota: Si hay múltiples registros el mismo día para un empleado, suma minutos.
    """
    df = df_out.copy()
    if "Fecha" not in df.columns:
        return pd.DataFrame()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])
    if len(df) == 0:
        return pd.DataFrame()

    df["Semana"] = df["Fecha"].apply(lambda x: _week_key(x, cfg))
    df["_min_trab"] = df.get("Horas trabajadas", "").apply(hhmm_to_minutes)
    df["_min_extra"] = df.get("Horas extra", "").apply(hhmm_to_minutes)

    for c in ["ID", "Nombre", "Semana"]:
        if c not in df.columns:
            return pd.DataFrame()

    dias_es = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    try:
        ws = int(getattr(cfg, "week_start_dow", 0) or 0) % 7
    except Exception:
        ws = 0
    dias_orden = dias_es[ws:] + dias_es[:ws]

    df["_dia"] = df["Fecha"].dt.dayofweek.map(lambda i: dias_es[int(i)] if pd.notna(i) else "")
    daily = df.groupby(["ID", "Nombre", "Semana", "_dia"], as_index=False).agg(
        Trab_min=("_min_trab", "sum"),
        Extra_min=("_min_extra", "sum"),
        Fecha_min=("Fecha", "min"),
    )
    daily["Horas_dia"] = daily["Trab_min"].apply(minutes_to_hhmm)

    pivot = daily.pivot_table(
        index=["ID", "Nombre", "Semana"],
        columns="_dia",
        values="Horas_dia",
        aggfunc="first",
    ).reset_index()

    for dname in dias_orden:
        if dname not in pivot.columns:
            pivot[dname] = ""
    pivot[dias_orden] = pivot[dias_orden].astype(object)

    totals = df.groupby(["ID", "Nombre", "Semana"], as_index=False).agg(
        Dias_presentes=("Fecha", lambda x: x.dt.date.nunique()),
        Total_trab_min=("_min_trab", "sum"),
        Total_extra_min=("_min_extra", "sum"),
    )
    totals["Total horas trabajadas"] = totals["Total_trab_min"].apply(minutes_to_hhmm)
    totals["Total horas extra"] = totals["Total_extra_min"].apply(minutes_to_hhmm)
    totals = totals.drop(columns=["Total_trab_min", "Total_extra_min"])

    if (
        faltas_semanal is not None
        and len(faltas_semanal) > 0
        and set(["ID", "Semana", "Faltas"]).issubset(faltas_semanal.columns)
    ):
        totals = totals.merge(faltas_semanal[["ID", "Semana", "Faltas"]], on=["ID", "Semana"], how="left")

    if "Faltas" not in totals.columns:
        totals["Faltas"] = 0
    totals["Faltas"] = pd.to_numeric(totals["Faltas"], errors="coerce").fillna(0).astype(int)

    try:
        fechas = df.groupby(["ID", "Semana"])["Fecha"].min().reset_index()
        fechas["Rango semana"] = fechas["Fecha"].apply(lambda f: rango_semana(pd.to_datetime(f), ws))
        totals = totals.merge(fechas[["ID", "Semana", "Rango semana"]], on=["ID", "Semana"], how="left")
    except Exception:
        totals["Rango semana"] = ""

    out = pivot.merge(totals, on=["ID", "Nombre", "Semana"], how="left")

    # Orden final de columnas determinístico
    fixed_cols = ["ID", "Nombre", "Semana"] + dias_orden + [
        "Dias_presentes",
        "Total horas trabajadas",
        "Total horas extra",
        "Faltas",
        "Rango semana",
    ]
    for c in fixed_cols:
        if c not in out.columns:
            out[c] = "" if c in dias_orden or c == "Rango semana" else 0
    out = out[fixed_cols]
    out["_grp_idx"] = out["ID"].map(lambda x: _grupo_sort_key(str(x), cfg)[0])
    out = out.sort_values(by=["_grp_idx", "ID", "Semana"], kind="stable").drop(columns=["_grp_idx"]).reset_index(drop=True)
    return out


def construir_resumen_mensual(df_out: pd.DataFrame, cfg, faltas_mensual: pd.DataFrame = None) -> pd.DataFrame:

    """Resumen mensual por ID: Días presentes, Faltas, Totales."""
    df = df_out.copy()
    if "Fecha" not in df.columns:
        return pd.DataFrame()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])
    if len(df)==0:
        return pd.DataFrame()
    df["Mes"] = df["Fecha"].apply(_month_key)
    df["_min_trab"] = df.get("Horas trabajadas","").apply(hhmm_to_minutes)
    df["_min_extra"] = df.get("Horas extra","").apply(hhmm_to_minutes)
    grp_cols = ["ID","Nombre","Mes"]
    for c in grp_cols:
        if c not in df.columns:
            return pd.DataFrame()

    g = df.groupby(grp_cols, dropna=False, as_index=False).agg(
        Dias_presentes=("Fecha", lambda x: x.dt.date.nunique()),
        Total_trab_min=("_min_trab","sum"),
        Total_extra_min=("_min_extra","sum"),
    )
    g["Total horas trabajadas"] = g["Total_trab_min"].apply(minutes_to_hhmm)
    g["Total horas extra"] = g["Total_extra_min"].apply(minutes_to_hhmm)
    g = g.drop(columns=["Total_trab_min","Total_extra_min"])
    if faltas_mensual is not None and len(faltas_mensual)>0 and set(["ID","Mes","Faltas"]).issubset(faltas_mensual.columns):
        g = g.merge(faltas_mensual[["ID","Mes","Faltas"]], on=["ID","Mes"], how="left")
    if "Faltas" not in g.columns:
        g["Faltas"]=0
    g["Faltas"] = pd.to_numeric(g["Faltas"], errors="coerce").fillna(0).astype(int)
    g["_grp_idx"] = g["ID"].map(lambda x: _grupo_sort_key(str(x), cfg)[0])
    g = g.sort_values(by=["_grp_idx","ID","Mes"], kind="stable").drop(columns=["_grp_idx"]).reset_index(drop=True)
    return g


def construir_resumen_semanal_vertical(df_out: pd.DataFrame, cfg, faltas_semanal: pd.DataFrame = None) -> pd.DataFrame:
    """Resumen semanal VERTICAL por ID: Días presentes, Faltas, Totales."""
    df = df_out.copy()
    if "Fecha" not in df.columns:
        return pd.DataFrame()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])
    if len(df)==0:
        return pd.DataFrame()
    df["Semana"] = df["Fecha"].apply(lambda x: _week_key(x, cfg))
    df["_min_trab"] = df.get("Horas trabajadas","").apply(hhmm_to_minutes)
    df["_min_extra"] = df.get("Horas extra","").apply(hhmm_to_minutes)
    grp_cols = ["ID","Nombre","Semana"]
    for c in grp_cols:
        if c not in df.columns:
            return pd.DataFrame()
    g = df.groupby(grp_cols, dropna=False, as_index=False).agg(
        Dias_presentes=("Fecha", lambda x: x.dt.date.nunique()),
        Total_trab_min=("_min_trab","sum"),
        Total_extra_min=("_min_extra","sum"),
    )
    g["Total horas trabajadas"] = g["Total_trab_min"].apply(minutes_to_hhmm)
    g["Total horas extra"] = g["Total_extra_min"].apply(minutes_to_hhmm)
    g = g.drop(columns=["Total_trab_min","Total_extra_min"])
    if faltas_semanal is not None and len(faltas_semanal)>0 and set(["ID","Semana","Faltas"]).issubset(faltas_semanal.columns):
        g = g.merge(faltas_semanal[["ID","Semana","Faltas"]], on=["ID","Semana"], how="left")
    if "Faltas" not in g.columns:
        g["Faltas"]=0
    g["Faltas"] = pd.to_numeric(g["Faltas"], errors="coerce").fillna(0).astype(int)
    g["_grp_idx"] = g["ID"].map(lambda x: _grupo_sort_key(str(x), cfg)[0])
    g = g.sort_values(by=["_grp_idx","ID","Semana"], kind="stable").drop(columns=["_grp_idx"]).reset_index(drop=True)
    # Agregar rango real de la semana (según cfg.week_start_dow)
    try:
        # tomar la primera fecha de la semana por ID
        fechas = df.groupby(["ID","Semana"])["Fecha"].min().reset_index()
        fechas["Rango semana"] = fechas["Fecha"].apply(lambda f: rango_semana(pd.to_datetime(f), cfg.week_start_dow))
        g = g.merge(fechas[["ID","Semana","Rango semana"]], on=["ID","Semana"], how="left")
    except Exception:
        pass
    return g


def crear_resumen_semanal_checadas(df_rep: pd.DataFrame, cfg: AppConfig, modo: str = "PROCESADO") -> pd.DataFrame:
    """
    Hoja de control interno: RESUMEN SEMANAL DE CHECADAS (por empleado).
    - Una fila por empleado por semana (según cfg.week_start_dow).
    - Columnas por día y por evento:
        Entrada, Salida a comer, Regreso de comer, Salida a cenar, Regreso de cenar, Salida
    - Para modo IDGRUPO, se omite columna ID (archivo orientado a IDGRUPO).
    Nota: No depende de que la columna "Semana" venga en formato clave; calcula semana desde "Fecha".
    """
    if df_rep is None or len(df_rep) == 0:
        return pd.DataFrame()
    eventos = ["Entrada", "Salida a comer", "Regreso de comer", "Salida a cenar", "Regreso de cenar", "Salida"]
    if "Fecha" not in df_rep.columns or "Nombre" not in df_rep.columns:
        return pd.DataFrame()
    if not set(eventos).issubset(set(df_rep.columns)):
        return pd.DataFrame()
    df = df_rep.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
    df = df.dropna(subset=["Fecha"])
    if len(df) == 0:
        return pd.DataFrame()
    # Semana según week_start_dow (por defecto 2=Miércoles para tu operación)

    try:
        dow = int(getattr(cfg, "week_start_dow", 2) or 2)
    except Exception:
        dow = 2
    dow = dow % 7
    def _week_start(dd):
        delta = (dd.weekday() - dow) % 7
        return dd - timedelta(days=delta)
    df["WSTART"] = df["Fecha"].apply(_week_start)
    df["Semana"] = df["WSTART"].apply(lambda ws: f"{ws.strftime('%Y-%m-%d')} a {(ws + timedelta(days=6)).strftime('%Y-%m-%d')}")
    df["DIA_OFF"] = df.apply(lambda r: (r["Fecha"] - r["WSTART"]).days, axis=1)
    df = df[(df["DIA_OFF"] >= 0) & (df["DIA_OFF"] <= 6)]
    if len(df) == 0:
        return pd.DataFrame()
    df["DIA_LABEL"] = df.apply(lambda r: f"{_dia_abrev_es((r['WSTART'] + timedelta(days=int(r['DIA_OFF']))).weekday())} {(r['WSTART'] + timedelta(days=int(r['DIA_OFF']))).strftime('%d/%m')}", axis=1)
    modo = (modo or "PROCESADO").upper().strip()
    if modo == "IDGRUPO":
        if "IDGRUPO" not in df.columns:
            return pd.DataFrame()
        keys = ["IDGRUPO", "Nombre", "Semana"]
    else:
        if "ID" not in df.columns:
            return pd.DataFrame()
        keys = ["ID", "Nombre", "Semana"]
    long = df[keys + ["DIA_LABEL"] + eventos].melt(
        id_vars=keys + ["DIA_LABEL"],
        value_vars=eventos,
        var_name="Evento",
        value_name="Hora",
    )
    long["Col"] = long["DIA_LABEL"].astype(str) + " " + long["Evento"].astype(str)
    wide = long.pivot_table(index=keys, columns="Col", values="Hora", aggfunc="first").reset_index()
    # Orden de columnas: 7 días (según WSTART) x eventos
    day_labels = [f"{_dia_abrev_es((date(2000,1,3) + timedelta(days=((dow + i) % 7 - 0))).weekday())}" for i in range(7)]  # dummy, luego se reemplaza
    try:
        ws0 = pd.to_datetime(df["WSTART"].min()).date()
        day_labels = [f"{_dia_abrev_es((ws0 + timedelta(days=i)).weekday())} {(ws0 + timedelta(days=i)).strftime('%d/%m')}" for i in range(7)]
    except Exception:
        day_labels = []
    ordered_cols = []
    for dl in day_labels:
        for ev in eventos:
            cname = f"{dl} {ev}"
            if cname in wide.columns:
                ordered_cols.append(cname)
    base_cols = keys
    remaining = [c for c in wide.columns if c not in base_cols and c not in ordered_cols]
    out = wide[base_cols + ordered_cols + remaining].copy()
    for c in out.columns:
        if c in base_cols:
            continue
        out[c] = out[c].fillna("")
    # Orden filas: dejar blancos al final
    if modo == "IDGRUPO":
        out["__SORT"] = out["IDGRUPO"].astype(str).replace({"": "ZZZ"}).fillna("ZZZ")
        out = out.sort_values(["Semana", "__SORT", "Nombre"]).drop(columns=["__SORT"])
    else:
        out["__SORT"] = out["ID"].astype(str).replace({"": "ZZZ"}).fillna("ZZZ")
        out = out.sort_values(["Semana", "__SORT", "Nombre"]).drop(columns=["__SORT"])
    return out
