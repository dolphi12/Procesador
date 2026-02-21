import pandas as pd

from procesador.groups import transform_sheet_procesado, transform_sheet_idgrupo
from procesador.config import AppConfig


SHEETS = ['Reporte', 'CONTROL', 'DETALLE_FALTAS', 'INCIDENCIAS_RRHH', 'RESUMEN_MENSUAL', 'RESUMEN_SEMANAL', 'RESUM_SEM_CHECADAS']


def test_contract_all_sheets_procesado_and_idgrupo():
    cfg = AppConfig()

    def idgrupo_of(_):
        return "000"

    for name in SHEETS:
        # dummy df representing a sheet output that initially contains both columns
        df = pd.DataFrame({"ID": ["001", "002"], "IDGRUPO": ["000-01", "000-02"], "X": [1, 2]})

        proc = transform_sheet_procesado(df)
        assert "ID" in proc.columns, f"PROCESADO sheet def construir_salida must keep ID"
        assert "IDGRUPO" not in proc.columns, f"PROCESADO sheet def construir_salida must not have IDGRUPO"

        idg = transform_sheet_idgrupo(df, cfg, idgrupo_of)
        assert "ID" not in idg.columns, f"IDGRUPO sheet def construir_salida must not have ID"
        assert "IDGRUPO" in idg.columns, f"IDGRUPO sheet def construir_salida must have IDGRUPO"
