
from datetime import time, date
from procesador.config import AppConfig
from procesador.pipeline import _recalcular_out_row

def _cfg():
    cfg = AppConfig()
    cfg.umbral_extra_min = 8 * 60
    cfg.tope_descuento_comida_min = 30
    cfg.umbral_comida_media_hora_min = 60
    cfg.redondeo_extra_step_min = 1
    cfg.redondeo_extra_modo = "none"
    return cfg

def test_cena_ignored_if_before_end_of_comida_due_to_edit():
    cfg = _cfg()
    emp_id="1"
    fecha=date(2026,1,1)
    # Times base: entrada, comida, regreso, salida
    times=[time(8,25), time(13,11), time(13,41), time(15,47)]
    # Corrección de eventos: el usuario puso "Salida a cenar" erróneamente antes de terminar comida
    correcciones_eventos={(emp_id, fecha): {"Salida a cenar": (time(10,38), "dedo")}}
    correcciones_nolabor={}
    out=_recalcular_out_row(
        emp_id=emp_id,
        nombre="X",
        fecha_val=fecha,
        semana="WK",
        pases="",
        registro_original="",
        times=times,
        cfg=cfg,
        run_id="R",
        usuario_editor="RRHH",
        audit_log=[],
        modo_seguro=False,
        correcciones_eventos=correcciones_eventos,
        correcciones_nolabor=correcciones_nolabor,
        ajuste_manual="No",
        nota_ajuste="",
    )
    assert out["Salida a cenar"] == "", "Se debe ignorar la cena anómala para evitar descuentos absurdos"
    assert "Cena anómala" in (out.get("Notas") or "")
