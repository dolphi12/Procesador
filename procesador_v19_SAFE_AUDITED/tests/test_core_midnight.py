from datetime import time

from procesador.core import normalize_registro_times, map_eventos, calcular_trabajado
from procesador.config import AppConfig


def t(h, m):
    return time(hour=h, minute=m)


def test_midnight_keeps_order_when_already_chronological():
    # Ya está en orden cronológico a través de medianoche: NO debe marcar reordenado
    times = [t(20, 48), t(23, 10), t(3, 5), t(6, 0)]
    norm, reord = normalize_registro_times(times)
    assert reord is False
    assert norm[0] == t(20, 48)
    assert norm[-1] == t(6, 0)


def test_midnight_reorders_when_out_of_order():
    # Desordenado (02:00 antes que 23:00) -> debe reordenar y respetar cruce
    times = [t(22, 0), t(2, 0), t(23, 0), t(6, 0)]
    norm, reord = normalize_registro_times(times)
    assert reord is True
    assert norm[0] == t(22, 0)
    assert norm[-1] == t(6, 0)


def test_map_eventos_caps_to_6():
    times = [t(8, 0), t(9, 0), t(10, 0), t(11, 0), t(12, 0), t(13, 0), t(17, 0)]
    ev = map_eventos(times)
    assert ev["Entrada"] == t(8, 0)
    assert ev["Salida"] == t(17, 0)


def test_calcular_trabajado_tope_comida():
    cfg = AppConfig()
    cfg.umbral_extra_min = 480
    cfg.tope_descuento_comida_min = 30
    ev = {
        "Entrada": t(9, 0),
        "Salida a comer": t(12, 0),
        "Regreso de comer": t(13, 0),  # 60 min
        "Salida a cenar": None,
        "Regreso de cenar": None,
        "Salida": t(18, 0),
    }
    trabajado, extra, comida, cena, nolab, ov_cd, ov_nolab, ign = calcular_trabajado(ev, cfg, None)
    assert comida == 30
    # 9h base=540 - 30 = 510 => extra=30 (umbral 480)
    assert trabajado == 510
    assert extra == 30

def test_calcular_trabajado_comida_excede_umbral_descuenta_completo():
    cfg = AppConfig()
    cfg.umbral_extra_min = 480
    cfg.tope_descuento_comida_min = 30
    cfg.umbral_comida_media_hora_min = 60
    ev = {
        "Entrada": t(9, 0),
        "Salida a comer": t(12, 0),
        "Regreso de comer": t(13, 10),  # 70 min => se descuenta completo
        "Salida a cenar": None,
        "Regreso de cenar": None,
        "Salida": t(18, 0),
    }
    trabajado, extra, comida, cena, nolab, ov_cd, ov_nolab, ign = calcular_trabajado(ev, cfg, None)
    assert comida == 70
    # base=540 - 70 = 470 => sin extra (umbral 480)
    assert trabajado == 470
    assert extra == 0
