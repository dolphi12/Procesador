from pathlib import Path
import pandas as pd
from procesador.pipeline import procesar_archivo


def test_verify_flag_ok(tmp_path: Path):
    inp = tmp_path / "in.xlsx"
    df = pd.DataFrame({
        "ID": ["115"],
        "Fecha": ["28/01/2026"],
        "Semana": ["1"],
        "Nombre": ["Empleado X"],
        "Número de pases de la tarjeta": ["1"],
        "Registro": ["09:00 13:00 13:30 18:00"],
    })
    df.to_excel(inp, index=False)

    out1, out2 = procesar_archivo(
        inp,
        correccion_interactiva=False,
        edicion_interactiva=False,
        usuario_editor="TEST",
        modo_seguro=True,
        verify=True,
    )
    assert out1.exists()
    assert out2.exists()
    # auditoría básica (latest.json) debe existir
    assert (tmp_path / "auditoria" / "latest.json").exists()
