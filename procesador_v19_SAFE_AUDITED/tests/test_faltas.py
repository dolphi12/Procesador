from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from procesador.faltas import calcular_faltas, cargar_plantilla_empleados
from procesador.config import AppConfig


def _make_plantilla(ids: list[str], activo: bool = True, fecha_baja: date | None = None) -> pd.DataFrame:
    """Construye un DataFrame de plantilla mínimo con la estructura esperada."""
    rows = []
    for emp_id in ids:
        row = {
            "ID": emp_id,
            "Nombre": f"Empleado {emp_id}",
            "Activo": "SI" if activo else "NO",
            "_activo": activo,
            "FechaAlta": None,
            "FechaBaja": fecha_baja,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def test_calcular_faltas_returns_three_dataframes():
    cfg = AppConfig()
    cfg.week_start_dow = 0  # lunes
    df_out = pd.DataFrame({"ID": ["001"], "Fecha": ["2026-01-05"]})
    plantilla = _make_plantilla(["001"])
    result = calcular_faltas(df_out, plantilla, cfg)
    assert isinstance(result, tuple)
    assert len(result) == 3
    for item in result:
        assert isinstance(item, pd.DataFrame)


def test_calcular_faltas_empleado_activo_con_faltas():
    cfg = AppConfig()
    cfg.week_start_dow = 0  # semana empieza lunes
    # Empleado "001" presente solo el lunes 2026-01-05; la semana va lun-dom
    # Espera faltas los días mar-dom (6 días)
    df_out = pd.DataFrame({
        "ID": ["001"],
        "Fecha": ["2026-01-05"],
        "Nombre": ["Empleado 001"],
    })
    plantilla = _make_plantilla(["001"])
    df_sem, df_mes, df_det = calcular_faltas(df_out, plantilla, cfg)
    assert len(df_det) > 0
    assert "ID" in df_det.columns
    assert "Fecha" in df_det.columns
    assert "Semana" in df_det.columns
    # El empleado 001 tiene faltas en la semana
    assert "001" in df_sem["ID"].values
    assert df_sem.loc[df_sem["ID"] == "001", "Faltas"].iloc[0] >= 1


def test_calcular_faltas_empleado_con_fecha_baja_anterior_al_rango():
    cfg = AppConfig()
    cfg.week_start_dow = 0
    # df_out tiene fechas en 2026-01-05..2026-01-09 para empleado "002"
    fechas = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    df_out = pd.DataFrame({
        "ID": ["002"] * 5,
        "Fecha": fechas,
        "Nombre": ["Empleado 002"] * 5,
    })
    # FechaBaja = 2026-01-04 (antes del rango) para empleado "001"
    plantilla = _make_plantilla(["001"], fecha_baja=date(2026, 1, 4))
    df_sem, df_mes, df_det = calcular_faltas(df_out, plantilla, cfg)
    # "001" fue dado de baja antes del rango: NO debe tener faltas
    assert "001" not in (df_det["ID"].values if len(df_det) > 0 else [])


def test_calcular_faltas_plantilla_none_retorna_vacios():
    cfg = AppConfig()
    df_out = pd.DataFrame({"ID": ["001"], "Fecha": ["2026-01-05"]})
    df_sem, df_mes, df_det = calcular_faltas(df_out, None, cfg)
    assert len(df_sem) == 0
    assert len(df_mes) == 0
    assert len(df_det) == 0


def test_calcular_faltas_plantilla_vacia_retorna_vacios():
    cfg = AppConfig()
    df_out = pd.DataFrame({"ID": ["001"], "Fecha": ["2026-01-05"]})
    df_sem, df_mes, df_det = calcular_faltas(df_out, pd.DataFrame(), cfg)
    assert len(df_sem) == 0
    assert len(df_mes) == 0
    assert len(df_det) == 0


def test_cargar_plantilla_empleados_fallback_desde_cfg(tmp_path: Path):
    cfg = AppConfig()
    cfg.empleado_status = {"042": {"activo": True}}
    cfg.empleado_meta = {"042": {"nombre": "Test Employee"}}
    # No hay archivo plantilla -> usa fallback desde cfg
    df = cargar_plantilla_empleados(
        script_dir=tmp_path,
        ruta="",
        cfg=cfg,
        empleados_detectados=["042"],
    )
    assert df is not None
    assert len(df) > 0
    assert "ID" in df.columns
    assert "_activo" in df.columns
