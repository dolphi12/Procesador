from datetime import date
from procesador.config import AppConfig
from procesador.parsers import parse_registro
from procesador.core import normalize_registro_times, map_eventos, calcular_trabajado


def _worked_minutes(registro: str, cfg: AppConfig) -> int:
    times = parse_registro(registro)
    times_norm, _reord = normalize_registro_times(times)
    eventos = map_eventos(times_norm)
    minutos_trabajados, *_resto = calcular_trabajado(eventos, cfg)
    return minutos_trabajados


def test_midnight_shift_simple_total():
    cfg = AppConfig()
    assert _worked_minutes("22:00 02:00", cfg) == 240


def test_midnight_with_seconds_and_breaks():
    cfg = AppConfig()
    assert _worked_minutes("23:50:12 00:10 00:40:59 02:00", cfg) == 100
