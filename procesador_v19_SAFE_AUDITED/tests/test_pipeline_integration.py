from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from procesador.pipeline import procesar_archivo


def _make_input_excel(path: Path) -> None:
    df = pd.DataFrame({
        "ID": ["3", "3", "7", "7"],
        "Fecha": ["2026-01-05", "2026-01-06", "2026-01-05", "2026-01-06"],
        "Semana": ["Lun", "Mar", "Lun", "Mar"],
        "Nombre": ["Empleado A", "Empleado A", "Empleado B", "Empleado B"],
        "Número de pases de la tarjeta": ["1", "1", "1", "1"],
        "Registro": [
            "09:00 13:00 13:30 18:00",
            "09:05 13:05 13:35 18:10",
            "08:00 12:00 13:00 17:00",
            "08:10 12:10 13:10 17:10",
        ],
    })
    df.to_excel(path, index=False)


def test_pipeline_dry_run_no_exception(tmp_path: Path):
    inp = tmp_path / "asistencia.xlsx"
    _make_input_excel(inp)
    # dry_run=True: no debe generar archivos de salida ni lanzar excepciones
    out_proc, out_idg = procesar_archivo(
        inp,
        no_interactive=True,
        dry_run=True,
        script_dir_override=tmp_path,
    )
    assert not out_proc.exists(), "_PROCESADO.xlsx no debe existir en dry_run"
    assert not out_idg.exists(), "_IDGRUPO.xlsx no debe existir en dry_run"


def test_pipeline_genera_archivos_de_salida(tmp_path: Path):
    inp = tmp_path / "asistencia.xlsx"
    _make_input_excel(inp)
    out_proc, out_idg = procesar_archivo(
        inp,
        no_interactive=True,
        dry_run=False,
        script_dir_override=tmp_path,
    )
    assert out_proc.exists(), "_PROCESADO.xlsx debe existir"
    assert out_idg.exists(), "_IDGRUPO.xlsx debe existir"
    # verificar que se puede leer el archivo generado
    df = pd.read_excel(out_proc, sheet_name="Reporte")
    assert len(df) > 0
