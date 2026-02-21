from pathlib import Path
import json

import pandas as pd

from procesador.pipeline import procesar_archivo
from procesador.corrections import verificar_auditoria_bundle
from procesador.config import cargar_config


def _make_input_excel(path: Path) -> None:
    df = pd.DataFrame(
        {
            "ID": ["3"],
            "Fecha": ["28/01/2026"],  # dd/mm para probar dayfirst
            "Nombre": ["Empleado A"],
            "Número de pases de la tarjeta": ["1"],
            "Registro": ["09:00 13:00 13:30 18:00"],
        }
    )
    df.to_excel(path, index=False)


def _make_plantilla(path: Path) -> None:
    df = pd.DataFrame(
        {
            "ID": ["03"],
            "Nombre": ["Empleado A"],
            "Activo": ["SI"],
            "IDGRUPO": ["000"],
        }
    )
    df.to_excel(path, index=False)


def test_audit_bundle_signed_and_verifiable(tmp_path: Path):
    inp = tmp_path / "mini.xlsx"
    plantilla = tmp_path / "plantilla.xlsx"
    _make_input_excel(inp)
    _make_plantilla(plantilla)

    out_proc, out_idg = procesar_archivo(
        inp,
        correccion_interactiva=False,
        plantilla_path=str(plantilla),
        edicion_interactiva=False,
        usuario_editor="TEST",
        modo_seguro=True,
    )

    audit_dir = tmp_path / "auditoria"
    assert audit_dir.exists()

    latest = json.loads((audit_dir / "latest.json").read_text(encoding="utf-8"))
    bundle_path = audit_dir / latest["bundle"]
    assert bundle_path.exists()

    script_dir = Path(__file__).resolve().parents[1] / "procesador"
    cfg = cargar_config(script_dir)

    assert verificar_auditoria_bundle(bundle_path, script_dir=script_dir, cfg=cfg) is True
