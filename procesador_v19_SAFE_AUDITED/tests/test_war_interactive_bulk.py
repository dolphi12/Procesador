from datetime import date

from procesador.corrections import editar_checadas_interactivo
from procesador.config import AppConfig


def test_interactive_bulk_plan(monkeypatch):
    cfg = AppConfig()
    audit_log = []

    inputs = iter([
        "2",  # Insertar checada
        "12:00",  # Hora a insertar
        "",   # Posición (Enter=final)
        "motivo_insert",
        "7",  # Bulk
        "2026-01-29..2026-01-30",
        "motivo_bulk",
        "6",  # Guardar
        "nota_final",
    ])

    def fake_input(_prompt: str = ""):
        return next(inputs)

    monkeypatch.setattr("builtins.input", fake_input)

    times, nota, bulk_plan, no_labor = editar_checadas_interactivo(
        run_id="RUN",
        emp_id="115",
        nombre="Empleado",
        fecha_d=date(2026, 1, 28),
        registro_raw="09:00 18:00",
        cfg=cfg,
        usuario="TEST",
        audit_log=audit_log,
        modo_seguro=False,
    )

    assert times is not None
    assert nota == "nota_final"
    assert bulk_plan is not None
    assert bulk_plan["emp_id"] == "115"
    assert bulk_plan["times"] == ["09:00", "12:00", "18:00"]
    assert bulk_plan["dates"] == ["2026-01-29", "2026-01-30"]
    assert isinstance(no_labor, list)
    assert len(audit_log) >= 2
