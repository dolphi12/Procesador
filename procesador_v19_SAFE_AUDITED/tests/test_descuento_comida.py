from datetime import time

from procesador.core import map_eventos, calcular_trabajado
from procesador.config import AppConfig


def t(h, m):
    return time(hour=h, minute=m)


def test_comida_descuenta_media_hora_si_no_excede_60():
    cfg = AppConfig()
    # Entrada 08:00, salida 17:00 -> 540 min base
    # Comida 12:00 a 13:00 (60 min) -> descuenta 30
    times = [t(8, 0), t(12, 0), t(13, 0), t(17, 0)]
    eventos = map_eventos(times)
    trabajado, extra, comida_ded, cena_ded, nolab_ded, *_ = calcular_trabajado(eventos, cfg, no_laborado_extra=None)
    assert comida_ded == 30
    assert cena_ded == 0
    assert trabajado == 540 - 30


def test_comida_descuenta_completo_si_excede_60():
    cfg = AppConfig()
    # Comida 12:00 a 13:10 (70 min) -> descuenta 70 completo
    times = [t(8, 0), t(12, 0), t(13, 10), t(17, 0)]
    eventos = map_eventos(times)
    trabajado, extra, comida_ded, cena_ded, nolab_ded, *_ = calcular_trabajado(eventos, cfg, no_laborado_extra=None)
    assert comida_ded == 70
    assert trabajado == 540 - 70


def test_cena_descuenta_completo():
    cfg = AppConfig()
    # Cena 18:00 a 18:20 -> descuenta 20
    times = [t(9, 0), t(13, 0), t(13, 30), t(18, 0), t(18, 20), t(19, 0)]
    eventos = map_eventos(times)
    trabajado, extra, comida_ded, cena_ded, nolab_ded, *_ = calcular_trabajado(eventos, cfg, no_laborado_extra=None)
    assert comida_ded == 30  # 13:00-13:30 => 30
    assert cena_ded == 20
