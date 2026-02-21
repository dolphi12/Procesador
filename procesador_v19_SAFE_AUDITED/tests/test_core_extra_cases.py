from datetime import time

from procesador.core import map_eventos, calcular_trabajado
from procesador.config import AppConfig


def t(h, m):
    return time(hour=h, minute=m)


def test_map_eventos_single_checada():
    ev = map_eventos([t(8, 0)])
    assert ev["Entrada"] == t(8, 0)
    assert ev["Salida"] is None
    assert ev["Salida a comer"] is None
    assert ev["_extra_registros"] == 0


def test_map_eventos_exactly_6_checadas():
    times = [t(8, 0), t(12, 0), t(13, 0), t(18, 0), t(18, 30), t(22, 0)]
    ev = map_eventos(times)
    assert ev["Entrada"] == t(8, 0)
    assert ev["Salida a comer"] == t(12, 0)
    assert ev["Regreso de comer"] == t(13, 0)
    assert ev["Salida a cenar"] == t(18, 0)
    assert ev["Regreso de cenar"] == t(18, 30)
    assert ev["Salida"] == t(22, 0)
    assert ev["_extra_registros"] == 0


def test_map_eventos_8_checadas_extra_registros():
    # 8 checadas -> extra_registros = max(0, 8 - 6) = 2
    times = [t(8, 0), t(9, 0), t(10, 0), t(11, 0), t(12, 0), t(13, 0), t(14, 0), t(17, 0)]
    ev = map_eventos(times)
    assert ev["_extra_registros"] == 2
    assert ev["Entrada"] == t(8, 0)
    assert ev["Salida"] == t(17, 0)
    # intermedias: solo las primeras 4 de las 6 intermedias son usadas
    assert ev["Salida a comer"] == t(9, 0)
    assert ev["Regreso de comer"] == t(10, 0)
    assert ev["Salida a cenar"] == t(11, 0)
    assert ev["Regreso de cenar"] == t(12, 0)


def test_calcular_trabajado_8_horas_exactas_sin_extra():
    cfg = AppConfig()
    cfg.umbral_extra_min = 480  # 8 horas exactas = umbral
    ev = {
        "Entrada": t(8, 0),
        "Salida a comer": None,
        "Regreso de comer": None,
        "Salida a cenar": None,
        "Regreso de cenar": None,
        "Salida": t(16, 0),
    }
    trabajado, extra, comida, cena, *_ = calcular_trabajado(ev, cfg, None)
    assert trabajado == 480
    assert extra == 0


def test_calcular_trabajado_9_horas_sin_comida_extra_60():
    cfg = AppConfig()
    cfg.umbral_extra_min = 480
    ev = {
        "Entrada": t(8, 0),
        "Salida a comer": None,
        "Regreso de comer": None,
        "Salida a cenar": None,
        "Regreso de cenar": None,
        "Salida": t(17, 0),
    }
    trabajado, extra, comida, cena, *_ = calcular_trabajado(ev, cfg, None)
    assert trabajado == 540
    assert extra == 60  # 540 - 480


def test_calcular_trabajado_comida_exactamente_en_umbral_descuenta_tope():
    # Comida exactamente 60 min (= umbral) -> descuenta tope (30 min)
    cfg = AppConfig()
    cfg.umbral_extra_min = 480
    cfg.tope_descuento_comida_min = 30
    cfg.umbral_comida_media_hora_min = 60
    ev = {
        "Entrada": t(8, 0),
        "Salida a comer": t(12, 0),
        "Regreso de comer": t(13, 0),  # exactamente 60 min
        "Salida a cenar": None,
        "Regreso de cenar": None,
        "Salida": t(17, 0),
    }
    trabajado, extra, comida, cena, *_ = calcular_trabajado(ev, cfg, None)
    assert comida == 30  # tope: 30 min
    assert trabajado == 540 - 30  # 510
