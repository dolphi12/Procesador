"""CLI del procesador (v19).

Subcomandos:
- process: procesa un archivo y exporta _PROCESADO.xlsx + _IDGRUPO.xlsx
- verify-audit: verifica la firma de un bundle de auditoría (HMAC-SHA256)

Compatibilidad:
- Si se invoca sin subcomando, se asume "process".
"""

from __future__ import annotations

import argparse
import json
import datetime
from pathlib import Path

from .config import cargar_config
from .corrections import verificar_auditoria_bundle
from .logger import setup_logging
from .pipeline import procesar_archivo
from .merge_inputs import collect_inputs, merge_inputs


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Procesador de asistencias (v19 modular).")
    sub = p.add_subparsers(dest="cmd")

    # ---- process ----
    p_proc = sub.add_parser("process", help="Procesar archivo de asistencias.")
    g_in = p_proc.add_mutually_exclusive_group(required=True)
    g_in.add_argument("--input", "--in", dest="input_path", default="", help="Archivo Excel/CSV de entrada.")
    g_in.add_argument("--input-dir", dest="input_dir", default="", help="Directorio con archivos diarios a consolidar (cierre semanal/mensual).")
    p_proc.add_argument("--pattern", dest="pattern", default="*.xlsx", help="Patrón de archivos dentro de --input-dir (default: *.xlsx).")
    p_proc.add_argument("--recursive", dest="recursive", action="store_true", help="Buscar archivos recursivamente dentro de --input-dir.")
    p_proc.add_argument("--merge-output", dest="merge_output", default="", help="(Opcional) Guardar el consolidado generado antes de procesar.")
    p_proc.add_argument("--plantilla", dest="plantilla", default="", help="Ruta plantilla_empleados.xlsx (opcional).")
    p_proc.add_argument("--interactive", dest="edicion_interactiva", action="store_true", help="Habilita edición interactiva.")
    p_proc.add_argument("--interactive-anomalias", dest="interactive_anomalias", action="store_true", help="Solo pregunta editar si detecta anomalías (segundos, duplicados, reordenado, sin horas, >6).")
    p_proc.add_argument("--interactive-grupos", dest="interactive_grupos", action="store_true", help="Permite editar mapeo empleado->grupo/IDGRUPO durante la corrida.")
    p_proc.add_argument("--review", dest="review_por_id", action="store_true", help="Al final, abrir dashboard para buscar por ID y editar checadas (aunque no haya anomalías).")
    p_proc.add_argument("--usuario", dest="usuario_editor", default="RRHH", help="Usuario que autoriza correcciones.")
    p_proc.add_argument("--modo-seguro", dest="modo_seguro", action="store_true", help="No aplicar heurísticas automáticas sin confirmación.")
    p_proc.add_argument("--verify", dest="verify", action="store_true", help="Valida artefactos y auditoría al final (corrida controlada).")
    p_proc.add_argument("--dry-run", dest="dry_run", action="store_true", help="Simula el procesamiento sin escribir archivos ni modificar estado/auditoría.")
    p_proc.add_argument("--no-interactive", dest="no_interactive", action="store_true", help="Modo batch: desactiva dashboards/prompts.")
    p_proc.add_argument("--audit-user", dest="audit_user", default="", help="Alias de --usuario (etiqueta auditoría).")
    p_proc.add_argument("--tests-only", dest="tests_only", action="store_true", help="Ejecuta pytest y termina (no procesa archivos).")
    p_proc.add_argument("--log-level", dest="log_level", default="INFO", help="DEBUG/INFO/WARNING/ERROR.")

    # ---- verify-audit ----
    p_ver = sub.add_parser("verify-audit", help="Verificar firma de auditoría (bundle).")
    g = p_ver.add_mutually_exclusive_group(required=True)
    g.add_argument("--bundle", dest="bundle_path", default="", help="Ruta a auditoria_run_<run_id>.json")
    g.add_argument("--latest", dest="latest_dir", default="", help="Directorio que contiene ./auditoria/latest.json")
    p_ver.add_argument("--log-level", dest="log_level", default="INFO", help="DEBUG/INFO/WARNING/ERROR.")

    # ---- merge ----
    p_merge = sub.add_parser("merge", help="Consolidar varios archivos diarios (inputs) en un solo Excel.")
    p_merge.add_argument("--input-dir", dest="input_dir", required=True, help="Directorio con archivos diarios.")
    p_merge.add_argument("--pattern", dest="pattern", default="*.xlsx", help="Patrón de archivos (default: *.xlsx).")
    p_merge.add_argument("--recursive", dest="recursive", action="store_true", help="Buscar recursivamente.")
    p_merge.add_argument("--output", dest="output_path", required=True, help="Archivo Excel consolidado de salida.")
    p_merge.add_argument("--no-dedupe", dest="no_dedupe", action="store_true", help="No eliminar duplicados por (ID, Nombre, Fecha, Registro).")
    p_merge.add_argument("--no-sort", dest="no_sort", action="store_true", help="No ordenar por Fecha/ID/Nombre.")
    p_merge.add_argument("--keep-extra-cols", dest="keep_extra_cols", action="store_true", help="Conservar columnas extra (si existen) al exportar.")
    p_merge.add_argument("--log-level", dest="log_level", default="INFO", help="DEBUG/INFO/WARNING/ERROR.")

    # Compat legacy: permitir flags de process sin subcomando
    p.add_argument("--input", "--in", dest="legacy_input_path", default="", help=argparse.SUPPRESS)
    p.add_argument("--plantilla", dest="legacy_plantilla", default="", help=argparse.SUPPRESS)
    p.add_argument("--interactive", dest="legacy_edicion_interactiva", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--interactive-anomalias", dest="legacy_interactive_anomalias", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--interactive-grupos", dest="legacy_interactive_grupos", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--review", dest="legacy_review_por_id", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--usuario", dest="legacy_usuario_editor", default="RRHH", help=argparse.SUPPRESS)
    p.add_argument("--modo-seguro", dest="legacy_modo_seguro", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--verify", dest="legacy_verify", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--log-level", dest="legacy_log_level", default="INFO", help=argparse.SUPPRESS)

    return p


def _cmd_process(args: argparse.Namespace) -> int:
    setup_logging(level=str(args.log_level).upper())

    # Compat: en modo legacy algunos atributos pueden no existir.
    audit_user = str(getattr(args, "audit_user", "") or "")
    usuario_editor = str(getattr(args, "usuario_editor", "RRHH") or "RRHH")

    # --tests-only: ejecuta solo la suite de tests y termina
    if bool(getattr(args, "tests_only", False)):
        try:
            import pytest  # type: ignore
        except Exception:
            print("ERROR: pytest no está disponible en este entorno.")
            return 2
        return int(pytest.main(["-q"]))

    # Determinar input: archivo directo o consolidado desde directorio
    if str(getattr(args, "input_dir", "")).strip():
        in_dir = Path(str(args.input_dir))
        files = collect_inputs(
            in_dir,
            pattern=str(getattr(args, "pattern", "*.xlsx")),
            recursive=bool(getattr(args, "recursive", False)),
        )
        if not files:
            print(f"ERROR: no se encontraron archivos en {in_dir} con patrón {getattr(args,'pattern','*.xlsx')}")
            return 2

        # output del merge
        if str(getattr(args, "merge_output", "")).strip():
            merged_path = Path(str(args.merge_output))
        else:
            stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            merged_path = in_dir / f"_MERGED_{stamp}.xlsx"

        rep = merge_inputs(files, merged_path, dedupe=True, sort=True, keep_extra_cols=False)
        in_path = rep.output_path
    else:
        in_path = Path(args.input_path)
        if not in_path.exists():
            print(f"ERROR: archivo de entrada no existe: {in_path}")
            return 2
        if in_path.is_dir():
            print(f"ERROR: se esperaba un archivo, no un directorio: {in_path}")
            return 2

    try:
        procesar_archivo(
            in_path,
            correccion_interactiva=bool(getattr(args, "interactive_grupos", False)),
            plantilla_path=args.plantilla,
            edicion_interactiva=bool(args.edicion_interactiva),
            usuario_editor=str(audit_user or usuario_editor),
            modo_seguro=bool(args.modo_seguro),
            verify=bool(getattr(args, "verify", False)),
            interactive_anomalias=bool(getattr(args, "interactive_anomalias", False)),
            interactive_grupos=bool(getattr(args, "interactive_grupos", False)),
            review_por_id=bool(getattr(args, "review_por_id", False)),
            dry_run=bool(getattr(args, "dry_run", False)),
            no_interactive=bool(getattr(args, "no_interactive", False)),
        )
    except RuntimeError as e:
        # --verify usa RuntimeError para marcar corrida inválida
        print(str(e))
        return 2
    return 0

def _cmd_merge(args: argparse.Namespace) -> int:
    setup_logging(level=str(args.log_level).upper())
    in_dir = Path(args.input_dir)
    files = collect_inputs(in_dir, pattern=str(args.pattern), recursive=bool(args.recursive))
    if not files:
        print(f"ERROR: no se encontraron archivos en {in_dir} con patrón {args.pattern}")
        return 2
    out_path = Path(args.output_path)
    rep = merge_inputs(
        files,
        out_path,
        dedupe=not bool(args.no_dedupe),
        sort=not bool(args.no_sort),
        keep_extra_cols=bool(args.keep_extra_cols),
    )
    print(f"OK: merge completado -> {rep.output_path}")
    print(f"Archivos: {rep.files_read} | Filas entrada: {rep.rows_in} | Filas salida: {rep.rows_out} | Duplicados removidos: {rep.duplicates_dropped}")
    return 0


def _cmd_verify_audit(args: argparse.Namespace) -> int:
    setup_logging(level=str(args.log_level).upper())
    # localizar bundle
    if args.bundle_path:
        bundle = Path(args.bundle_path)
        if not bundle.exists():
            print(f"ERROR: bundle no encontrado: {bundle}")
            return 2
        if bundle.is_dir():
            print(f"ERROR: se esperaba un archivo de bundle, no un directorio: {bundle}")
            return 2
    else:
        base = Path(args.latest_dir)
        audit_dir = base / "auditoria"
        latest = audit_dir / "latest.json"
        if not latest.exists():
            print(f"ERROR: no existe {latest}")
            return 2
        try:
            latest_text = latest.read_text(encoding="utf-8")
        except OSError:
            print(f"ERROR: no se pudo leer {latest}")
            return 2
        try:
            obj = json.loads(latest_text)
            raw_bundle = obj.get("bundle", "")
            if not isinstance(raw_bundle, str):
                raise ValueError("bundle debe ser string")
            bundle_name = raw_bundle.strip()
        except json.JSONDecodeError:
            print("ERROR: latest.json inválido (JSON)")
            return 2
        except ValueError:
            print("ERROR: bundle debe ser string en latest.json")
            return 2
        bundle_rel = Path(bundle_name)
        if (
            not bundle_name
            or bundle_rel.is_absolute()
            or ".." in bundle_rel.parts
        ):
            print("ERROR: bundle inválido en latest.json")
            return 2
        audit_dir_resolved = audit_dir.resolve()
        bundle = (audit_dir / bundle_rel).resolve()
        try:
            bundle.relative_to(audit_dir_resolved)
        except ValueError:
            print("ERROR: bundle inválido en latest.json")
            return 2
        if not bundle.exists():
            print(f"ERROR: bundle no encontrado: {bundle}")
            return 2
        if bundle.is_dir():
            print(f"ERROR: se esperaba un archivo de bundle, no un directorio: {bundle}")
            return 2

    script_dir = Path(__file__).resolve().parent
    cfg = cargar_config(script_dir)
    ok = verificar_auditoria_bundle(bundle, script_dir=script_dir, cfg=cfg)
    if ok:
        print("OK: firma válida")
        return 0
    print("ERROR: firma inválida o auditoría sin firma")
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Compat: si no hay subcomando pero se pasó --input, asumir process
    if not args.cmd and getattr(args, "legacy_input_path", ""):
        class A:  # shim
            pass
        a = A()
        a.input_path = args.legacy_input_path
        a.plantilla = args.legacy_plantilla
        a.edicion_interactiva = bool(args.legacy_edicion_interactiva)
        a.usuario_editor = str(args.legacy_usuario_editor)
        a.modo_seguro = bool(args.legacy_modo_seguro)
        a.verify = bool(getattr(args, 'legacy_verify', False))
        a.interactive_anomalias = bool(getattr(args, 'legacy_interactive_anomalias', False))
        a.interactive_grupos = bool(getattr(args, 'legacy_interactive_grupos', False))
        a.review_por_id = bool(getattr(args, 'legacy_review_por_id', False))
        a.log_level = str(args.legacy_log_level)
        return _cmd_process(a)

    if args.cmd == "process":
        return _cmd_process(args)
    if args.cmd == "verify-audit":
        import json
        return _cmd_verify_audit(args)
    if args.cmd == "merge":
        return _cmd_merge(args)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
