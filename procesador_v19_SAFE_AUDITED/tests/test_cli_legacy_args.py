from __future__ import annotations


def test_cmd_process_handles_missing_audit_user(monkeypatch):
    """Modo legacy puede no tener args.audit_user; no debe tronar."""

    from procesador import cli

    called = {"ok": False}

    def _fake_procesar_archivo(*args, **kwargs):
        called["ok"] = True

    monkeypatch.setattr(cli, "procesar_archivo", _fake_procesar_archivo)

    class Args:
        log_level = "INFO"
        tests_only = False
        input_path = "dummy.xlsx"
        plantilla = ""
        edicion_interactiva = False
        usuario_editor = "RRHH"
        modo_seguro = False
        verify = False
        interactive_anomalias = False
        interactive_grupos = False
        review_por_id = False
        dry_run = True
        no_interactive = True

    # Nota: NO definimos audit_user a propósito

    rc = cli._cmd_process(Args())
    assert rc == 0
    assert called["ok"] is True
