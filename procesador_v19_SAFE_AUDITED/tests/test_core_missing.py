from datetime import time

from procesador.core import calcular_trabajado
from procesador.config import AppConfig


def t(h, m):
    return time(hour=h, minute=m)


def test_calcular_trabajado_missing_entry_or_exit():
    cfg = AppConfig()
    ev = {"Entrada": None, "Salida": t(18, 0)}
    trabajado, extra, comida, cena, nolab, ov_cd, ov_nolab, ign = calcular_trabajado(ev, cfg, None)
    assert trabajado == 0
    assert extra == 0
    assert comida == 0
