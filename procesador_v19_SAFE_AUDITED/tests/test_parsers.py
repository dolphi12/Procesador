from datetime import time

from procesador.parsers import parse_time, parse_date, parse_registro


def test_parse_time_hhmm():
    t = parse_time("9:10")
    assert t == time(9, 10)


def test_parse_time_hhmmss():
    t = parse_time("09:10:59")
    assert t == time(9, 10)


def test_parse_time_invalid():
    assert parse_time("24:00") is None
    assert parse_time("aa:bb") is None


def test_parse_date_mixed():
    assert parse_date("2026-01-30").isoformat() == "2026-01-30"
    assert parse_date("30/01/2026").isoformat() == "2026-01-30"


def test_parse_registro_dedup_and_order():
    times = parse_registro("08:00; 08:00; 17:00; 09:00")
    assert [f"{t.hour:02d}:{t.minute:02d}" for t in times] == ["08:00", "17:00", "09:00"]
