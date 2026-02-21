import pandas as pd
from procesador.io import _sanitize_excel_injection

def test_sanitize_excel_injection_prefixes():
    df = pd.DataFrame({
        "Nombre": ["=HYPERLINK('x','y')", "+SUM(1,2)", "-1", "@cmd", "Normal", None],
        "Num": [1,2,3,4,5,6],
    })
    out = _sanitize_excel_injection(df)
    assert str(out.loc[0,"Nombre"]).startswith("'=")
    assert str(out.loc[1,"Nombre"]).startswith("'+")
    assert str(out.loc[2,"Nombre"]).startswith("'-")
    assert str(out.loc[3,"Nombre"]).startswith("'@")
    assert out.loc[4,"Nombre"] == "Normal"
    assert pd.isna(out.loc[5,"Nombre"]) or out.loc[5,"Nombre"] is None
    assert out["Num"].tolist() == [1,2,3,4,5,6]
