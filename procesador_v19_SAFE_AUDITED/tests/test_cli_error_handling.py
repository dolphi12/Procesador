from __future__ import annotations

from pathlib import Path


def _make_process_args(**overrides):
    class Args:
        log_level = "INFO"
        tests_only = False
        input_path = ""
        plantilla = ""
        edicion_interactiva = False
        usuario_editor = "RRHH"
        modo_seguro = False
        verify = False
        interactive_anomalias = False
        interactive_grupos = False
        review_por_id = False
        dry_run = False
        no_interactive = True
        input_dir = ""
        pattern = "*.xlsx"
        recursive = False
        merge_output = ""

    for k, v in overrides.items():
        setattr(Args, k, v)
    return Args()


def _make_verify_args(**overrides):
    class Args:
        bundle_path = ""
        latest_dir = ""
        log_level = "INFO"

    for k, v in overrides.items():
        setattr(Args, k, v)
    return Args()


def test_cmd_process_missing_file(monkeypatch, tmp_path):
    from procesador import cli

    called = {"procesar": 0}

    def _fake_procesar_archivo(*args, **kwargs):
        called["procesar"] += 1

    monkeypatch.setattr(cli, "procesar_archivo", _fake_procesar_archivo)

    missing = tmp_path / "nope.xlsx"
    args = _make_process_args(input_path=str(missing))
    rc = cli._cmd_process(args)
    assert rc == 2
    assert called["procesar"] == 0


def test_cmd_verify_audit_missing_latest(monkeypatch, tmp_path):
    from procesador import cli

    def _fail_verificar(*args, **kwargs):
        raise AssertionError("should not verify when latest.json is missing")

    monkeypatch.setattr(cli, "verificar_auditoria_bundle", _fail_verificar)

    args = _make_verify_args(latest_dir=str(tmp_path))
    rc = cli._cmd_verify_audit(args)
    assert rc == 2


def test_cmd_verify_audit_missing_bundle(monkeypatch, tmp_path):
    from procesador import cli

    def _fail_verificar(*args, **kwargs):
        raise AssertionError("should not verify when bundle is missing")

    monkeypatch.setattr(cli, "verificar_auditoria_bundle", _fail_verificar)

    bundle = tmp_path / "missing.json"
    args = _make_verify_args(bundle_path=str(bundle))
    rc = cli._cmd_verify_audit(args)
    assert rc == 2
