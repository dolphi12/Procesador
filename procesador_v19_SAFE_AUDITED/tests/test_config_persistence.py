from pathlib import Path

from procesador.config import AppConfig, cargar_config, guardar_config


def test_config_persists_status_and_meta(tmp_path: Path):
    cfg = AppConfig()
    cfg.audit_key_storage = "script"  # no tocar appdata del runner
    cfg.empleado_status = {"001": {"activo": False, "fecha_baja": "2026-01-31"}}
    cfg.empleado_meta = {"001": {"nombre": "Empleado 1"}}
    cfg.excel_idgrupo_split_by_group = True

    guardar_config(tmp_path, cfg)
    cfg2 = cargar_config(tmp_path)

    assert cfg2.empleado_status.get("001", {}).get("activo") is False
    assert cfg2.empleado_status.get("001", {}).get("fecha_baja") == "2026-01-31"
    assert cfg2.empleado_meta.get("001", {}).get("nombre") == "Empleado 1"
    assert cfg2.excel_idgrupo_split_by_group is True


def test_config_no_backup_when_unchanged(tmp_path: Path):
    cfg = AppConfig()
    cfg.audit_key_storage = "script"
    guardar_config(tmp_path, cfg)

    # Segunda escritura sin cambios no debe crear backups
    before = list(tmp_path.glob("mapa_grupos.json.bak_*") )
    guardar_config(tmp_path, cfg)
    after = list(tmp_path.glob("mapa_grupos.json.bak_*") )

    assert len(after) == len(before)
