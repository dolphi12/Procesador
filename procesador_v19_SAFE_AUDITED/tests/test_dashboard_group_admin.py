from datetime import date
from pathlib import Path

import pytest

from procesador.config import AppConfig, guardar_config, cargar_config
from procesador.corrections import editar_checadas_interactivo


def test_dashboard_option_10_group_admin_does_not_crash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Config mínima aislada
    cfg = AppConfig()
    cfg.audit_key_storage = "script"
    guardar_config(tmp_path, cfg)

    # Inputs:
    # - dashboard: 10 (admin grupos)
    # - admin: 7 (guardar y salir)
    # - dashboard: 0 (salir sin guardar checadas)
    inputs = iter(["10", "7", "0"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(inputs))

    emp_id = "001"
    fecha = date(2026, 1, 30)
    registro_raw = "09:00 18:00"

    times_edit, nota_final, bulk_plan, no_labor_out = editar_checadas_interactivo(
        run_id="TEST-RUN",
        emp_id=emp_id,
        nombre="Emp",
        fecha_d=fecha,
        registro_raw=registro_raw,
        registro_display=registro_raw,
        cfg=cargar_config(tmp_path),
        usuario="TEST",
        audit_log=[],
        modo_seguro=True,
        script_dir=tmp_path,
        processed_ids=[emp_id],
        no_labor=None,
    )

    assert times_edit is None
    assert bulk_plan is None
    assert isinstance(nota_final, str)
    assert isinstance(no_labor_out, list)
