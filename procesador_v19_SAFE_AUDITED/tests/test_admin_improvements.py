"""Tests for dashboard/admin improvements."""
from pathlib import Path

import pytest

from procesador.config import AppConfig, guardar_config, cargar_config
from procesador.group_admin import run_group_admin, _list_missing, _delete_employee


def _make_cfg(tmp_path: Path, emps: dict | None = None) -> AppConfig:
    cfg = AppConfig()
    cfg.audit_key_storage = "script"
    if emps:
        for emp_id, grp in emps.items():
            cfg.empleado_a_grupo[emp_id] = grp
            cfg.empleado_a_idgrupo[emp_id] = f"000-{emp_id}"
            cfg.empleado_status[emp_id] = {"activo": True}
    guardar_config(tmp_path, cfg)
    return cargar_config(tmp_path)


def test_admin_header_shows_mapped_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys):
    """Admin menu header must show 'En mapa: N' alongside procesados and sin mapa."""
    cfg = _make_cfg(tmp_path, {"001": "000", "002": "000"})

    # Enter admin, then immediately exit (option 0)
    inputs = iter(["0"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs))

    run_group_admin(
        script_dir=tmp_path,
        cfg=cfg,
        processed_ids=["001", "002", "003"],
        usuario="TEST",
    )

    out = capsys.readouterr().out
    assert "En mapa: 2" in out
    assert "Sin mapa: 1" in out
    assert "Empleados procesados: 3" in out


def test_delete_employee_shows_details(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys):
    """Deleting an employee must show group/IDGRUPO/status before confirmation."""
    cfg = _make_cfg(tmp_path, {"001": "000"})

    # Flow: option 3 (delete), select employee 1, decline (N)
    inputs = iter(["3", "1", "N", "0"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs))

    run_group_admin(
        script_dir=tmp_path,
        cfg=cfg,
        processed_ids=["001"],
        usuario="TEST",
    )

    out = capsys.readouterr().out
    assert "Grupo: 000" in out
    assert "IDGRUPO: 000-001" in out
    assert "ACTIVO" in out


def test_cascada_uses_processed_ids_not_dashboard_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Regression: _editar_cascada must use processed_ids param, not
    processed_ids_for_dashboard (which would cause NameError)."""
    import pandas as pd
    import procesador.pipeline as pipeline

    cfg = _make_cfg(tmp_path)

    df_out = pd.DataFrame([
        {"ID": "881", "Fecha": "2026-01-30", "Nombre": "Test",
         "Registro original": "09:00 18:00",
         "Registros normalizados": "09:00, 18:00",
         "Horas trabajadas": "09:00", "Horas extra": "00:00",
         "Discrepancias": ""},
    ])

    # Flow: option 1 (search by ID), enter "881", then select day 1,
    # exit editor (0), then Q cascade, then 5 (exit dashboard)
    inputs = iter(["1", "881", "1", "0", "q", "5"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs))

    # This MUST NOT raise NameError for processed_ids_for_dashboard
    out = pipeline.dashboard_revision_por_id(
        df_out=df_out,
        cfg=cfg,
        run_id="TEST",
        usuario_editor="RRHH",
        audit_log=[],
        modo_seguro=True,
        correcciones_eventos={},
        correcciones_nolabor={},
        script_dir=tmp_path,
        processed_ids=["881"],
    )
    assert isinstance(out, pd.DataFrame)


def test_list_missing_finds_ungrouped():
    """_list_missing must detect employees without group or IDGRUPO."""
    cfg = AppConfig()
    cfg.empleado_a_grupo = {"001": "000"}
    cfg.empleado_a_idgrupo = {"001": "000-001"}

    missing = _list_missing(["001", "002"], cfg)
    assert "002" in missing
    assert missing["002"] == "SIN_GRUPO"
    assert "001" not in missing


def test_list_missing_detects_no_idgrupo():
    """_list_missing must report SIN_IDGRUPO when group exists but IDGRUPO is empty."""
    cfg = AppConfig()
    cfg.empleado_a_grupo = {"001": "000"}
    cfg.empleado_a_idgrupo = {}

    missing = _list_missing(["001"], cfg)
    assert "001" in missing
    assert missing["001"] == "SIN_IDGRUPO"
