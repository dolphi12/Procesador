import pandas as pd
from pathlib import Path


def test_dashboard_revision_por_id_exits_without_crash(monkeypatch):
    """Regression: dashboard review must not crash due to missing script_dir/processed_ids."""
    import procesador.pipeline as pipeline
    from procesador.config import cargar_config

    script_dir = Path(pipeline.__file__).resolve().parent
    cfg = cargar_config(script_dir)

    df_out = pd.DataFrame([{"ID": "881", "Discrepancias": ""}])

    monkeypatch.setattr("builtins.input", lambda prompt="": "5")

    out = pipeline.dashboard_revision_por_id(
        df_out=df_out,
        cfg=cfg,
        run_id="TEST",
        usuario_editor="RRHH",
        audit_log=[],
        modo_seguro=True,
        correcciones_eventos={},
        correcciones_nolabor={},
        script_dir=script_dir,
        processed_ids=["881"],
    )
    assert isinstance(out, pd.DataFrame)
