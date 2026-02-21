from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from procesador import cli


def _make_input_excel(path: Path) -> None:
    df = pd.DataFrame({
        "ID": ["5"],
        "Fecha": ["2026-01-05"],
        "Nombre": ["Empleado Test"],
        "Número de pases de la tarjeta": ["1"],
        "Registro": ["09:00 13:00 13:30 18:00"],
    })
    df.to_excel(path, index=False)


def test_cli_dry_run_retorna_0_y_no_genera_archivos(tmp_path: Path):
    excel_path = tmp_path / "entrada.xlsx"
    _make_input_excel(excel_path)
    rc = cli.main(["process", "--input", str(excel_path), "--dry-run", "--no-interactive"])
    assert rc == 0
    procesado = tmp_path / "entrada_PROCESADO.xlsx"
    idgrupo = tmp_path / "entrada_IDGRUPO.xlsx"
    assert not procesado.exists(), "_PROCESADO.xlsx no debe crearse con --dry-run"
    assert not idgrupo.exists(), "_IDGRUPO.xlsx no debe crearse con --dry-run"


def test_cli_no_interactive_no_pide_input(monkeypatch, tmp_path: Path):
    excel_path = tmp_path / "entrada.xlsx"
    _make_input_excel(excel_path)
    # patch input/builtins.input para detectar si se invoca
    called = {"ok": False}

    def _fake_input(prompt=""):
        called["ok"] = True
        return ""

    monkeypatch.setattr("builtins.input", _fake_input)
    rc = cli.main(["process", "--input", str(excel_path), "--no-interactive", "--dry-run"])
    assert rc == 0
    assert not called["ok"], "No debe invocarse input() con --no-interactive"


def test_cli_merge_genera_archivo_consolidado(tmp_path: Path):
    df1 = pd.DataFrame({
        "ID": ["1"],
        "Nombre": ["A"],
        "Fecha": ["2026-01-05"],
        "Número de pases de la tarjeta": [1],
        "Registro": ["09:00 18:00"],
    })
    df2 = pd.DataFrame({
        "ID": ["2"],
        "Nombre": ["B"],
        "Fecha": ["2026-01-06"],
        "Número de pases de la tarjeta": [1],
        "Registro": ["08:00 17:00"],
    })
    f1 = tmp_path / "dia1.xlsx"
    f2 = tmp_path / "dia2.xlsx"
    df1.to_excel(f1, index=False)
    df2.to_excel(f2, index=False)

    out = tmp_path / "consolidado.xlsx"
    rc = cli.main([
        "merge",
        "--input-dir", str(tmp_path),
        "--output", str(out),
    ])
    assert rc == 0
    assert out.exists()
    merged = pd.read_excel(out)
    assert len(merged) == 2
