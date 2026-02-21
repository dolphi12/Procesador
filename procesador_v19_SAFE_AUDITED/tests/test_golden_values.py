import pandas as pd
from pathlib import Path

from procesador.pipeline import procesar_archivo


def _make_input_excel(path: Path) -> None:
    df = pd.DataFrame(
        {
            "ID": ["3","3","115","115","124","124","999"],
            "Fecha": [
                "2026-01-28","2026-01-29",
                "2026-01-28","2026-01-29",
                "2026-01-28","2026-01-29",
                "2026-01-28",
            ],
            "Nombre": [
                "Empleado A","Empleado A",
                "Empleado B","Empleado B",
                "Empleado C","Empleado C",
                "Empleado X",
            ],
            "Número de pases de la tarjeta": ["1","1","1","1","1","1","1"],
            "Registro": [
                "09:00 13:00 13:30 18:00",         # 9:00-18:00 - 0:30 comida = 8:30, extra=0:30
                "09:05 13:05 13:35 18:10",         # 9:05-18:10 - 0:30 = 8:35, extra=0:35
                "22:00 02:00",                      # nocturno: 4:00
                "22:10 02:05",                      # nocturno: 3:55
                "09:00:30 09:00:30 18:00:15",       # segundos + duplicados -> 09:00 a 18:00 => 9:00, extra=1:00
                "09:00 13:00",                      # falta salida final real; toma 13:00 como salida => 4:00, extra=0:00
                "09:00",                            # solo entrada => trabajado=0
            ],
        }
    )
    df.to_excel(path, index=False)


def _make_plantilla(path: Path) -> None:
    df = pd.DataFrame(
        {
            "ID": ["03", "115", "124", "999"],
            "Nombre": ["Empleado A", "Empleado B", "Empleado C", "Empleado X"],
            "Activo": ["SI", "SI", "SI", "SI"],
            "IDGRUPO": ["000", "F", "FT", "000"],
        }
    )
    df.to_excel(path, index=False)


def _read_reporte(xlsx: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx, sheet_name="Reporte", dtype=str).fillna("")
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date.astype(str)
    return df


def _row(df: pd.DataFrame, emp_id: str, fecha: str) -> dict:
    r = df[(df["ID"] == emp_id) & (df["Fecha"] == fecha)]
    assert len(r) == 1, f"Esperaba 1 fila para ID={emp_id} Fecha={fecha}, obtuve {len(r)}"
    return r.iloc[0].to_dict()


def test_golden_values_procesado(tmp_path: Path):
    inp = tmp_path / "mini_asistencia.xlsx"
    plantilla = tmp_path / "plantilla_empleados.xlsx"
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

    df = _read_reporte(out_proc)

    # Empleado A
    r = _row(df, "003", "2026-01-28")
    assert r["Entrada"] == "09:00"
    assert r["Salida"] == "18:00"
    assert r["Salida a comer"] == "13:00"
    assert r["Regreso de comer"] == "13:30"
    assert r["Horas trabajadas"] == "08:30"
    assert r["Horas extra"] == "00:30"

    r = _row(df, "003", "2026-01-29")
    assert r["Horas trabajadas"] == "08:35"
    assert r["Horas extra"] == "00:35"

    # Empleado B nocturno
    r = _row(df, "115", "2026-01-28")
    assert r["Entrada"] == "22:00"
    assert r["Salida"] == "02:00"
    assert r["Horas trabajadas"] == "04:00"
    assert r["Horas extra"] == "00:00"

    r = _row(df, "115", "2026-01-29")
    assert r["Horas trabajadas"] == "03:55"

    # Empleado C: segundos + duplicados
    r = _row(df, "124", "2026-01-28")
    assert r["Entrada"] == "09:00"
    assert r["Salida"] == "18:00"
    assert r["Horas trabajadas"] == "09:00"
    assert r["Horas extra"] == "01:00"

    # Empleado C: falta "salida final" (toma última checada como salida)
    r = _row(df, "124", "2026-01-29")
    assert r["Entrada"] == "09:00"
    assert r["Salida"] == "13:00"
    assert r["Horas trabajadas"] == "04:00"
    assert r["Horas extra"] == "00:00"

    # Empleado X: solo una checada => no rompe, trabajado=0
    r = _row(df, "999", "2026-01-28")
    assert r["Entrada"] == "09:00"
    assert r["Salida"] == ""
    assert r["Horas trabajadas"] == "00:00"
    assert r["Horas extra"] == "00:00"

    # --- Comparación contra IDGRUPO: mismos valores excepto identificador
    df_idg = pd.read_excel(out_idg, sheet_name="Reporte", dtype=str).fillna("")
    assert "ID" not in df_idg.columns
    assert "IDGRUPO" in df_idg.columns

    # Verificar una fila representativa (FT-124, 2026-01-28)
    df_idg["Fecha"] = pd.to_datetime(df_idg["Fecha"], errors="coerce").dt.date.astype(str)
    ri = df_idg[(df_idg["IDGRUPO"] == "FT-124") & (df_idg["Fecha"] == "2026-01-28")]
    assert len(ri) == 1
    ri = ri.iloc[0].to_dict()
    assert ri["Horas trabajadas"] == "09:00"
    assert ri["Horas extra"] == "01:00"
