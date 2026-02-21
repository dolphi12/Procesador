import json
from datetime import datetime
from pathlib import Path

from procesador.config import AppConfig
from procesador.corrections import AuditEntry, guardar_auditoria_bundle


def test_guardar_auditoria_bundle_escribe_auditoria_cambios_jsonl(tmp_path: Path):
    cfg = AppConfig()
    cfg.audit_signing_enabled = False  # evita depender de llave en tests

    run_id = "test_run_001"
    now = datetime.now().isoformat(timespec="seconds")

    audit_log = [
        AuditEntry(
            run_id=run_id,
            emp_id="001",
            fecha="2026-01-30",
            usuario="QA",
            ts=now,
            accion="EDIT",
            campo="Salida a comer",
            antes="09:29",
            despues="09:30",
            motivo="Test",
        ),
        AuditEntry(
            run_id=run_id,
            emp_id="001",
            fecha="2026-01-30",
            usuario="QA",
            ts=now,
            accion="INSERT",
            campo="Regreso de comer",
            antes="",
            despues="09:31",
            motivo="Test",
        ),
    ]

    out_dir = tmp_path / "salidas"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_meta = {
        "run_id": run_id,
        "started_at": now,
        "input_sha256": "deadbeef",
        "usuario": "QA",
    }

    guardar_auditoria_bundle(
        out_dir=out_dir,
        script_dir=tmp_path,
        audit_log=audit_log,
        run_meta=run_meta,
        cfg=cfg,
    )

    audit_dir = out_dir / cfg.audit_dir_name
    changes_path = audit_dir / cfg.audit_changes_filename
    assert changes_path.exists(), "Debe escribir auditoria_cambios.jsonl"

    lines = changes_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 2
    objs = [json.loads(x) for x in lines[-2:]]
    assert objs[0]["accion"] == "EDIT"
    assert objs[1]["accion"] == "INSERT"
