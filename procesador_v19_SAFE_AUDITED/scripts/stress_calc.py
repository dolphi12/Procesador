"""Stress test for calculation logic (food / dinner / no-laborado / extras).

This script generates random event sequences (including midnight crossing) and random
no-laborado intervals, then asserts basic invariants.

Usage:
    python scripts/stress_calc.py
"""

from __future__ import annotations

import random
import sys
from datetime import time
from pathlib import Path

# Ensure project root is on sys.path so "python scripts/..." works.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from procesador.config import AppConfig  # noqa: E402
from procesador.core import calcular_trabajado  # noqa: E402


def t(mins: int) -> time:
    mins = mins % (24 * 60)
    return time(minute=mins % 60, hour=mins // 60)


def main() -> int:
    random.seed(1337)
    cfg = AppConfig()

    # Generate 10k scenarios
    for _ in range(10_000):
        # Choose shift start anywhere, duration 4..14 hours
        start = random.randint(0, 23 * 60)
        dur = random.randint(4 * 60, 14 * 60)
        end = start + dur

        # Meal windows inside shift (might be missing)
        def maybe_window(min_len: int, max_len: int, prob: float):
            if random.random() > prob:
                return None, None
            s = start + random.randint(30, max(30, dur - 60))
            length = random.randint(min_len, max_len)
            e = s + length
            return t(s), t(e)

        salida_comer, regreso_comer = maybe_window(10, 90, 0.8)
        salida_cenar, regreso_cenar = maybe_window(5, 60, 0.6)

        eventos = {
            "entrada": t(start),
            "salida": t(end),
            "salida_comer": salida_comer,
            "regreso_comer": regreso_comer,
            "salida_cenar": salida_cenar,
            "regreso_cenar": regreso_cenar,
        }

        # NoLaborado random: 0..2 intervals inside shift
        nl = []
        for _k in range(random.randint(0, 2)):
            ns = start + random.randint(0, dur)
            ne = ns + random.randint(5, 90)
            nl.append((t(ns), t(ne), "NL"))

        (trab, extra, d_comida, d_cena, d_nol, *_rest) = calcular_trabajado(eventos, cfg, nl)

        # Invariants
        if not (0 <= trab <= 24 * 60):
            raise AssertionError(f"trab out of range: {trab}")
        if not (0 <= extra <= 24 * 60):
            raise AssertionError(f"extra out of range: {extra}")
        if extra > trab:
            raise AssertionError(f"extra > trab: {extra} > {trab}")
        for name, v in ("comida", d_comida), ("cena", d_cena), ("nol", d_nol):
            if v < 0:
                raise AssertionError(f"negative descuento {name}: {v}")

    print("[OK] stress_calc: 10,000 escenarios sin violaciones")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
