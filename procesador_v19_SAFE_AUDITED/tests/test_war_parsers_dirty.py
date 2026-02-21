import pytest
from procesador.config import AppConfig
from procesador.parsers import parse_time, parse_date


@pytest.mark.parametrize("raw, expected", [
    ("9:1", "09:01"),
    ("09:01:59", "09:01"),
    ("  7:05  ", "07:05"),
    ("", None),
    (None, None),
    ("24:10", None),
    ("aa:bb", None),
])
def test_parse_time_dirty(raw, expected):
    t = parse_time(raw)
    if expected is None:
        assert t is None
    else:
        assert t.strftime("%H:%M") == expected


@pytest.mark.parametrize("raw, dayfirst, expected", [
    ("28/01/2026", True, "2026-01-28"),
    ("01/02/2026", True, "2026-02-01"),
    ("01/02/2026", False, "2026-01-02"),
    ("2026-01-28", True, "2026-01-28"),
])
def test_parse_date_dayfirst(raw, dayfirst, expected):
    cfg = AppConfig()
    cfg.dayfirst = dayfirst
    d = parse_date(raw, cfg)
    assert d is not None
    assert d.isoformat() == expected
