from procesador.groups import build_idgrupo_label
from procesador.config import AppConfig


def test_build_idgrupo_label_padding():
    cfg = AppConfig()
    cfg.idgrupo_emp_min_width = 2
    cfg.idgrupo_sep = "-"
    assert build_idgrupo_label("000", 3, cfg) == "000-03"
    assert build_idgrupo_label("F", 115, cfg) == "F-115"
    assert build_idgrupo_label("FT", "124", cfg) == "FT-124"


def test_build_idgrupo_label_empty_code():
    cfg = AppConfig()
    assert build_idgrupo_label("", 3, cfg) == ""
