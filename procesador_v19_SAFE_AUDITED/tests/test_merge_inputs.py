from __future__ import annotations

from pathlib import Path

import pandas as pd

from procesador.merge_inputs import collect_inputs, merge_inputs


def test_merge_inputs_dedupe_and_sort(tmp_path: Path) -> None:
    # crear dos archivos diarios con duplicado
    df1 = pd.DataFrame(
        [
            {"ID": "12", "Nombre": "A", "Fecha": "2026-02-02", "Número de pases de la tarjeta": 1, "Registro": "08:00 12:00 13:00 17:00"},
            {"ID": "A", "Nombre": "A", "Fecha": "2026-02-02", "Número de pases de la tarjeta": 1, "Registro": "22:00 02:00 02:30 06:00"},
        ]
    )
    df2 = pd.DataFrame(
        [
            # duplicado exacto del primero
            {"ID": "12", "Nombre": "A", "Fecha": "2026-02-02", "Número de pases de la tarjeta": 1, "Registro": "08:00 12:00 13:00 17:00"},
            # siguiente día
            {"ID": "12", "Nombre": "A", "Fecha": "2026-02-03", "Número de pases de la tarjeta": 1, "Registro": "08:05 12:00 13:00 17:10"},
        ]
    )
    f1 = tmp_path / "d1.xlsx"
    f2 = tmp_path / "d2.xlsx"
    df1.to_excel(f1, index=False)
    df2.to_excel(f2, index=False)

    out = tmp_path / "merged.xlsx"
    rep = merge_inputs([f2, f1], out, dedupe=True, sort=True, keep_extra_cols=False)
    assert rep.files_read == 2
    assert rep.duplicates_dropped == 1
    assert out.exists()

    merged = pd.read_excel(out)
    # columnas mínimas
    assert list(merged.columns) == ["ID", "Nombre", "Fecha", "Número de pases de la tarjeta", "Registro"]
    # orden por fecha
    fechas = merged["Fecha"].astype(str).tolist()
    assert fechas == sorted(fechas)
    assert len(merged) == 3


def test_collect_inputs_pattern(tmp_path: Path) -> None:
    (tmp_path / "a.xlsx").write_text("x")
    (tmp_path / "b.xlsx").write_text("y")
    (tmp_path / "c.csv").write_text("z")
    files = collect_inputs(tmp_path, pattern="*.xlsx", recursive=False)
    assert [p.name for p in files] == ["a.xlsx", "b.xlsx"]


def test_merge_inputs_no_dedupe_conserva_duplicados(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {"ID": "1", "Nombre": "A", "Fecha": "2026-03-01", "Número de pases de la tarjeta": 1, "Registro": "08:00 17:00"},
            {"ID": "1", "Nombre": "A", "Fecha": "2026-03-01", "Número de pases de la tarjeta": 1, "Registro": "08:00 17:00"},
        ]
    )
    f = tmp_path / "dup.xlsx"
    df.to_excel(f, index=False)
    out = tmp_path / "merged_nodup.xlsx"
    rep = merge_inputs([f], out, dedupe=False)
    assert rep.duplicates_dropped == 0
    merged = pd.read_excel(out)
    assert len(merged) == 2
