import pandas as pd

from procesador.io import _drop_export_debug_cols

def test_export_drops_debug_columns_case_insensitive():
    df = pd.DataFrame({
        "ID": [1],
        "Registro original": ["08:00;12:00"],
        "Registros parseados": ["['08:00','12:00']"],
        "Registros normalizados": ["08:00,12:00"],
        "Fuente checadas": ["CHECADOR"],
        "Discrepancias": ["OK"],
        "Otra": ["x"],
    })
    out = _drop_export_debug_cols(df)
    assert "Registro original" not in out.columns
    assert "Registros parseados" not in out.columns
    assert "Registros normalizados" not in out.columns
    assert "Fuente checadas" not in out.columns
    assert "Discrepancias" not in out.columns
    assert "ID" in out.columns
    assert "Otra" in out.columns

def test_export_drop_tolerates_missing_columns():
    df = pd.DataFrame({"ID":[1], "Registro":["08:00"]})
    out = _drop_export_debug_cols(df)
    assert list(out.columns) == ["ID","Registro"]
