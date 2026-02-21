import pandas as pd
from procesador.groups import transform_sheet_procesado, transform_sheet_idgrupo
from procesador.config import AppConfig


def test_transform_sheet_procesado_drops_idgrupo():
    df = pd.DataFrame({"ID":["001"], "IDGRUPO":["000-01"], "X":[1]})
    out = transform_sheet_procesado(df)
    assert "ID" in out.columns
    assert "IDGRUPO" not in out.columns


def test_transform_sheet_idgrupo_adds_and_drops_id():
    cfg = AppConfig()
    def idgrupo_of(x):
        return "000"
    df = pd.DataFrame({"ID":[3,115], "X":[1,2]})
    out = transform_sheet_idgrupo(df, cfg, idgrupo_of)
    assert "ID" not in out.columns
    assert "IDGRUPO" in out.columns
    assert out["IDGRUPO"].tolist() == ["000-03", "000-115"]
