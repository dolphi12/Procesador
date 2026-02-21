
from datetime import time
from procesador.config import AppConfig
from procesador.core import calcular_trabajado

def _cfg():
    cfg = AppConfig()
    # asegurar defaults coherentes
    cfg.umbral_extra_min = 8 * 60
    cfg.tope_descuento_comida_min = 30
    cfg.umbral_comida_media_hora_min = 60
    cfg.redondeo_extra_step_min = 1
    cfg.redondeo_extra_modo = "none"
    return cfg

def test_nolabor_merge_cross_midnight_is_correct():
    cfg = _cfg()
    eventos = {
        "Entrada": time(22, 0),
        "Salida": time(6, 0),
    }
    # Intervalos NoLaborado solapados cruzando medianoche
    no_lab = [
        (time(23, 30), time(0, 30), "A"),
        (time(0, 20), time(1, 0), "B"),
    ]
    trabajado, extra, comida_ded, cena_ded, no_lab_ded, nolab_ov, nolab_intov, nolab_ign = calcular_trabajado(eventos, cfg, no_lab)
    assert comida_ded == 0
    assert cena_ded == 0
    # merged: 23:30 -> 01:00 = 90 min, overlap interno = 10 min (00:20-00:30)
    assert no_lab_ded == 90
    assert nolab_intov == 10
    assert nolab_ov == 0
    assert nolab_ign == 0
    # total turno: 480; trabajado: 390; extra: max(0, 390-480)=0
    assert trabajado == 480 - 90
    assert extra == 0

def test_nolabor_overlap_with_comida_not_double_discounted():
    cfg = _cfg()
    eventos = {
        "Entrada": time(22, 0),
        "Salida": time(6, 0),
        "Salida a comer": time(23, 45),
        "Regreso de comer": time(0, 25),  # 40 min real <= 60 => descuenta 30 (23:45-00:15 ventana)
    }
    no_lab = [
        (time(23, 50), time(0, 10), "solapa comida"),  # 20 min completamente dentro ventana 23:45-00:15
    ]
    trabajado, extra, comida_ded, cena_ded, no_lab_ded, nolab_ov, nolab_intov, nolab_ign = calcular_trabajado(eventos, cfg, no_lab)
    assert comida_ded == 30
    assert cena_ded == 0
    assert no_lab_ded == 0, "No debe descontar doble si ya cae dentro de la ventana de comida descontada"
    assert nolab_ov == 20
    assert nolab_intov == 0
    assert nolab_ign == 0
