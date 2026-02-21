import os
import json
from pathlib import Path

import pandas as pd

from procesador.config import AppConfig
from procesador.summaries import (
    construir_resumen_semanal,
    construir_resumen_mensual,
    construir_resumen_semanal_vertical,
    crear_resumen_semanal_checadas,
)
from procesador.groups import transform_sheet_procesado, transform_sheet_idgrupo


SNAP_PATH = Path(__file__).parent / "snapshots" / "headers_snapshot.json"


def _dummy_df_out() -> pd.DataFrame:
    # DataFrame representativo mínimo para que summaries generen columnas
    return pd.DataFrame(
        {
            "ID": ["001", "002"],
            "Nombre": ["A", "B"],
            "Fecha": ["2026-01-28", "2026-01-29"],
            "Horas trabajadas": ["08:00", "07:30"],
            "Horas extra": ["00:00", "00:30"],
            "Entrada": ["09:00", "09:00"],
            "Salida a comer": ["13:00", ""],
            "Regreso de comer": ["13:30", ""],
            "Salida a cenar": ["", ""],
            "Regreso de cenar": ["", ""],
            "Salida": ["18:00", "17:30"],
        }
    )


def _build_headers(cfg: AppConfig) -> dict:
    df_out = _dummy_df_out()
    # Ensure proper types are acceptable; summaries handle strings
    semanal = construir_resumen_semanal(df_out, cfg, faltas_semanal=pd.DataFrame({"ID":["001"],"Semana":[df_out["Fecha"].iloc[0]],"Faltas":[0]}))
    mensual = construir_resumen_mensual(df_out, cfg, faltas_mensual=pd.DataFrame({"ID":["001"],"Mes":[df_out["Fecha"].iloc[0][:7]],"Faltas":[0]}))
    semanal_v = construir_resumen_semanal_vertical(df_out, cfg, faltas_semanal=pd.DataFrame())
    sem_chec = crear_resumen_semanal_checadas(df_out, cfg, modo="PROCESADO")

    # Simular hojas exportables y aplicar transform por contrato
    hojas_procesado = {
        "Reporte": transform_sheet_procesado(df_out),
        "RESUMEN_SEMANAL": transform_sheet_procesado(semanal),
        "RESUMEN_MENSUAL": transform_sheet_procesado(mensual),
        "RESUMEN_SEMANAL_VERTICAL": transform_sheet_procesado(semanal_v),
        "RESUM_SEM_CHECADAS": transform_sheet_procesado(sem_chec),
    }

    def idgrupo_of(_):
        return "000"

    hojas_idgrupo = {
        k: transform_sheet_idgrupo(v, cfg, idgrupo_of)
        for k, v in hojas_procesado.items()
    }

    return {
        "PROCESADO": {k: list(v.columns) for k, v in hojas_procesado.items()},
        "IDGRUPO": {k: list(v.columns) for k, v in hojas_idgrupo.items()},
    }


def test_headers_snapshot():
    cfg = AppConfig()

    current = _build_headers(cfg)

    if os.getenv("UPDATE_SNAPSHOTS") == "1" or not SNAP_PATH.exists():
        SNAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        SNAP_PATH.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        # pass test after update
        assert True
        return

    expected = json.loads(SNAP_PATH.read_text(encoding="utf-8"))
    assert current == expected
