"""Cálculo de faltas (v18-compatible).

Separamos el cálculo de faltas y la carga de plantilla de empleados.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from .utils import normalize_id, _week_key, _month_key
from .groups import make_emp_key

def cargar_plantilla_empleados(script_dir: Path, ruta: str = "", cfg: 'object | None' = None, empleados_detectados: 'list[str] | None' = None) -> "pd.DataFrame | None":
    """Carga plantilla de empleados activos (Opción A).
    Mínimo: columna ID. Opcionales: Nombre, Activo (SI/NO), FechaAlta, FechaBaja.
    """
    path = Path(ruta) if ruta else (script_dir / "plantilla_empleados.xlsx")
    if not path.exists():
        # FALLBACK: construir plantilla interna desde config (dashboard) si es posible
        try:
            if cfg is not None:
                ids = set()
                for x in (empleados_detectados or []):
                    s = str(x).strip()
                    if s and not s.startswith("NOMBRE::"):
                        ids.add(s)
                for x in (getattr(cfg, "empleado_status", {}) or {}).keys():
                    s = str(x).strip()
                    if s and not s.startswith("NOMBRE::"):
                        ids.add(s)
                # si no hay nada, cae al comportamiento previo
                if ids:
                    rows = []
                    for emp in sorted(ids):
                        meta = (getattr(cfg, "empleado_meta", {}) or {}).get(emp, {}) or {}
                        st = (getattr(cfg, "empleado_status", {}) or {}).get(emp, {}) or {}
                        activo = bool(st.get("activo", True))
                        nombre = str(meta.get("nombre", "") or "")
                        idgrupo = str((getattr(cfg, "empleado_a_idgrupo", {}) or {}).get(emp, "") or "")
                        rows.append({"ID": emp, "Nombre": nombre, "Activo": "SI" if activo else "NO", "IDGRUPO": idgrupo})
                    df = pd.DataFrame(rows)
                    # Normalizar como plantilla
                    df["ID"] = df["ID"].astype(str).str.strip()
                    df["ID"] = df["ID"].apply(lambda x: normalize_id(x, 3))
                    df["ID"] = df.apply(lambda r: make_emp_key(r.get("ID",""), r.get("Nombre",""), 3)[0], axis=1)
                    df["Nombre"] = df["Nombre"].astype(str).fillna("").astype(str)
                    if "IDGRUPO" in df.columns:
                        df["IDGRUPO"] = df["IDGRUPO"].astype(str).fillna("").astype(str).str.strip()
                    def _is_active(x):
                        s = str(x or "").strip().lower()
                        return s in ("si","sí","s","1","true","activo","active","yes","y")
                    df["_activo"] = df["Activo"].apply(_is_active)
                    # fechas opcionales
                    for c in ("FechaAlta","FechaBaja"):
                        if c not in df.columns:
                            df[c] = ""
                    return df
        except Exception:
            pass
        print(f"[AVISO] No se encontró la plantilla de empleados: {path}.")
        print("        Se omitirá el cálculo de FALTAS.")
        print("        (Solución: coloca plantilla_empleados.xlsx junto al script o usa --plantilla.)")
        return None
    try:
        df = pd.read_excel(path, dtype=str)
    except Exception:
        print(f"[AVISO] No se pudo leer la plantilla de empleados: {path}.")
        print("        Se omitirá el cálculo de FALTAS.")
        return None
    df.columns = [str(c).replace('\ufeff','').strip() for c in df.columns]
    id_col = None
    for c in df.columns:
        if str(c).strip().lower() in ("id","id_empleado","empleado","employeeid"):
            id_col = c
            break
    if id_col is None:
        return pd.DataFrame()
    df = df.rename(columns={id_col:"ID"})
    df["ID"] = df["ID"].astype(str).str.strip()
    df["ID"] = df["ID"].apply(lambda x: normalize_id(x, 3))
    # Caso SIN ID: si ID es texto (nombre) lo convertimos a clave interna NOMBRE:: para que coincida con el export.
    df["ID"] = df.apply(lambda r: make_emp_key(r.get("ID",""), r.get("Nombre",""), 3)[0], axis=1)
    # OVERRIDE_CONFIG: si cfg trae estatus/nombre, aplicar sobre plantilla
    try:
        if cfg is not None:
            meta_map = getattr(cfg, "empleado_meta", {}) or {}
            st_map = getattr(cfg, "empleado_status", {}) or {}
            if isinstance(meta_map, dict) and "Nombre" in df.columns:
                for i, r in df.iterrows():
                    eid = str(r.get("ID","")).strip()
                    m = meta_map.get(eid, {}) or {}
                    if (not str(r.get("Nombre","") or "").strip()) and m.get("nombre"):
                        df.at[i, "Nombre"] = str(m.get("nombre","")).strip()
            if isinstance(st_map, dict):
                if "Activo" not in df.columns:
                    df["Activo"] = "SI"
                for i, r in df.iterrows():
                    eid = str(r.get("ID","")).strip()
                    st = st_map.get(eid)
                    if isinstance(st, dict) and ("activo" in st):
                        df.at[i, "Activo"] = "SI" if bool(st.get("activo", True)) else "NO"
    except Exception:
        pass
    if "Nombre" not in df.columns:
        for c in df.columns:
            if str(c).strip().lower() in ("nombre","name"):
                df = df.rename(columns={c:"Nombre"})
                break
    if "Nombre" not in df.columns:
        df["Nombre"] = ""

    # Columna opcional: IDGRUPO (ej. FT-123). Si existe, se conserva para alimentar el reporte *_IDGRUPO.xlsx
    if "IDGRUPO" not in df.columns:
        for c in df.columns:
            if str(c).strip().lower() in ("idgrupo","id_grupo","grupo","grupo_id","id grupo"):
                df = df.rename(columns={c:"IDGRUPO"})
                break
    if "IDGRUPO" not in df.columns:
        df["IDGRUPO"] = ""
    else:
        df["IDGRUPO"] = df["IDGRUPO"].astype(str).fillna("").str.strip()
    if "Activo" not in df.columns:
        for c in df.columns:
            if str(c).strip().lower() in ("activo","active","estatus","status"):
                df = df.rename(columns={c:"Activo"})
                break
    if "Activo" not in df.columns:
        df["Activo"] = "SI"
    def _is_active(x):
        s = str(x).strip().lower()
        return s in ("si","sí","s","1","true","activo","active","yes","y")
    df["_activo"] = df["Activo"].apply(_is_active)
    for col in ("FechaAlta","FechaBaja"):
        if col not in df.columns:
            for c in df.columns:
                if str(c).strip().lower() == col.lower():
                    df = df.rename(columns={c:col})
                    break
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        else:
            df[col] = pd.NaT
    df = df[df["ID"] != ""]
    return df


def calcular_faltas(df_out: pd.DataFrame, plantilla: pd.DataFrame, cfg: AppConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Calcula faltas por semana y mes + detalle, usando plantilla (empleados activos).
    Fechas esperadas: rango completo Fecha_min..Fecha_max del archivo procesado.
    """
    if df_out is None or len(df_out)==0 or plantilla is None or len(plantilla)==0:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    if "Fecha" not in df_out.columns:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    df = df_out.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.dropna(subset=["Fecha"])
    if len(df)==0:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    min_d = df["Fecha"].min().date()
    max_d = df["Fecha"].max().date()
    # Expandir rango esperado a semanas completas según inicio configurado.
    # Esto permite detectar FALTAS aunque proceses un archivo de 1 solo día.
    def _week_start(d: date) -> date:
        dow = d.weekday()
        delta = (dow - int(getattr(cfg, "week_start_dow", 0))) % 7
        return d - timedelta(days=delta)
    def _week_end(d: date) -> date:
        return _week_start(d) + timedelta(days=6)
    exp_ini = _week_start(min_d)
    exp_fin = _week_end(max_d)
    fechas = pd.date_range(exp_ini, exp_fin, freq="D").date
    pad = lambda v: normalize_id(v, 3)
    presentes = set((pad(r.get("ID","")), r["Fecha"].date()) for _, r in df.iterrows() if str(r.get("ID","")).strip()!="")
    nombre_por_id = {}
    if "Nombre" in df.columns:

        for _, r in df[["ID","Nombre"]].dropna().drop_duplicates().iterrows():
            i = pad(r.get("ID",""))
            if i and i not in nombre_por_id:
                nombre_por_id[i] = str(r.get("Nombre","") or "")
    for _, r in plantilla.iterrows():
        i = pad(r.get("ID",""))
        n = str(r.get("Nombre","") or "")
        if i and n:
            nombre_por_id[i]=n
    detalles=[]
    for _, emp in plantilla.iterrows():
        emp_id = pad(emp.get("ID",""))
        if not emp_id or not bool(emp.get("_activo", True)):
            continue
        alta = emp.get("FechaAlta"); baja = emp.get("FechaBaja")
        for d in fechas:
            if pd.notna(alta) and alta and d < alta: 
                continue
            if pd.notna(baja) and baja and d > baja:
                continue
            if (emp_id, d) not in presentes:
                ts = pd.Timestamp(d)
                detalles.append({"ID":emp_id,"Nombre":nombre_por_id.get(emp_id,""),"Fecha":ts.date(),
                                 "Semana":_week_key(ts, cfg),"Mes":_month_key(ts)})
    df_det = pd.DataFrame(detalles)
    if len(df_det)==0:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    df_sem = df_det.groupby(["ID","Nombre","Semana"], as_index=False).agg(Faltas=("Fecha","count"))
    df_mes = df_det.groupby(["ID","Nombre","Mes"], as_index=False).agg(Faltas=("Fecha","count"))
    return (df_sem, df_mes, df_det)
