"""Corrections and audit trail (interactive editing + append-only history).

This module provides:
- AuditEntry schema (stable)
- guardar_auditoria_json(): JSON history for the current run
- guardar_auditoria_bundle(): per-run signed bundle + append-only index + latest pointer
- verificar_auditoria_bundle(): verify bundle signature
- editar_checadas_interactivo(): textual dashboard to edit punches during batch processing

Design goals:
- Robustness with dirty data
- Append-only audit (who/when/what/why)
- Safe interactivity (EOF-safe input)

"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, asdict
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .audit import log_many
from .utils import (
    chmod_restringido,
    default_app_data_dir,
    get_or_create_audit_key,
    harden_permissions,
    hmac_sha256_hex,
    verify_hmac_sha256_hex,
)


def _legacy():
    # Lazy import to avoid circular imports (legacy imports corrections).
    from . import legacy  # type: ignore

    return legacy



def _resolve_audit_key_dir(script_dir: Path, cfg: Any) -> Path:
    """Decide dónde guardar/leer la llave de firma.

    Default: directorio de datos de la app (no junto al código).
    Override:
    - cfg.audit_key_dir: ruta absoluta o relativa
    - cfg.audit_key_storage in {'script','local','codedir'}: usar script_dir
    """
    cfg_dir = str(getattr(cfg, "audit_key_dir", "") or "").strip()
    if cfg_dir:
        return Path(cfg_dir).expanduser().resolve()
    storage = str(getattr(cfg, "audit_key_storage", "appdata") or "appdata").strip().lower()
    if storage in {"script", "local", "codedir"}:
        return Path(script_dir)
    appname = str(getattr(cfg, "app_name", "procesador") or "procesador")
    return default_app_data_dir(appname=appname)

@dataclass(slots=True)
class AuditEntry:
    """Append-only audit entry for punch edits."""

    run_id: str
    emp_id: str
    fecha: str  # YYYY-MM-DD
    usuario: str
    ts: str  # ISO8601
    accion: str  # EDIT/INSERT/DELETE/REVERT/SAVE/BULK_APPLY
    campo: str
    antes: object
    despues: object
    motivo: str


def guardar_auditoria_json(path: Path, audit_log: List[AuditEntry]) -> None:
    """Write audit log as JSON (best-effort) and tighten permissions."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = [asdict(a) for a in (audit_log or [])]
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    harden_permissions(path)


def _rotate_if_needed(path: Path, max_bytes: int) -> None:
    """Rota el archivo si excede max_bytes (best-effort)."""

    try:
        path = Path(path)
        if not path.exists():
            return
        if max_bytes and path.stat().st_size > int(max_bytes):
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated = path.with_name(path.stem + f"_{ts}" + path.suffix)
            path.rename(rotated)
            try:
                chmod_restringido(rotated)
            except Exception:
                pass
    except Exception:
        return


def guardar_auditoria_bundle(
    *,
    out_dir: Path,
    script_dir: Path,
    audit_log: List[AuditEntry],
    run_meta: Dict[str, object],
    cfg: Any,
) -> Dict[str, Path]:
    """Guarda auditoría con:

    - Bundle por corrida (JSON): auditoria_run_<run_id>.json (meta + entries + firma)
    - Índice append-only (JSONL): auditoria_index.jsonl (una línea por corrida)
    - Latest pointer: latest.json

    Retorna paths creados.
    """

    out_dir = Path(out_dir)
    audit_dir = out_dir / getattr(cfg, "audit_dir_name", "auditoria")
    audit_dir.mkdir(parents=True, exist_ok=True)
    try:
        chmod_restringido(audit_dir)
    except Exception:
        pass

    run_id = str(run_meta.get("run_id") or "")
    bundle_path = audit_dir / f"auditoria_run_{run_id}.json"
    index_path = audit_dir / str(getattr(cfg, "audit_index_filename", "auditoria_index.jsonl"))
    latest_path = audit_dir / "latest.json"

    _rotate_if_needed(index_path, int(getattr(cfg, "audit_rotate_max_bytes", 0) or 0))

    entries = [asdict(a) for a in (audit_log or [])]
    payload_obj = {"meta": run_meta, "entries": entries}
    payload_bytes = json.dumps(
        payload_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    signature = ""
    key_id = ""
    algo = "NONE"
    if bool(getattr(cfg, "audit_signing_enabled", True)):
        try:
            key_dir = _resolve_audit_key_dir(script_dir, cfg)
            key_hex, key_id = get_or_create_audit_key(
                script_dir,
                filename=str(getattr(cfg, "audit_key_filename", "audit_key.txt")),
                key_dir=key_dir,
                appname=str(getattr(cfg, "app_name", "procesador") or "procesador"),
            )
            signature = hmac_sha256_hex(key_hex, payload_bytes)
            algo = "HMAC-SHA256"
        except Exception:
            signature = ""
            key_id = ""
            algo = "NONE"

    bundle_obj = {
        "meta": run_meta,
        "entries": entries,
        "signature_algo": algo,
        "key_id": key_id,
        "signature": signature,
    }
    bundle_path.write_text(json.dumps(bundle_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        chmod_restringido(bundle_path)
    except Exception:
        pass

    # Append index line (no entries)
    index_line = {
        "run_id": run_id,
        "ts": run_meta.get("started_at"),
        "input_sha256": run_meta.get("input_sha256"),
        "usuario": run_meta.get("usuario"),
        "bundle": bundle_path.name,
        "signature_algo": algo,
        "key_id": key_id,
        "signature": signature,
    }
    with open(index_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(index_line, ensure_ascii=False) + "\n")
    try:
        chmod_restringido(index_path)
    except Exception:
        pass

    latest_path.write_text(
        json.dumps({"run_id": run_id, "bundle": bundle_path.name}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    try:
        chmod_restringido(latest_path)
    except Exception:
        pass


    # Append auditoria_cambios.jsonl (detalle por edición, append-only)
    try:
        recs: List[Dict[str, object]] = []
        for a in (audit_log or []):
            recs.append(
                {
                    "run_id": run_id,
                    "emp_id": a.emp_id,
                    "fecha": a.fecha,
                    "usuario": a.usuario,
                    "ts": a.ts,
                    "accion": a.accion,
                    "campo": a.campo,
                    "antes": a.antes,
                    "despues": a.despues,
                    "motivo": a.motivo,
                }
            )
        if recs:
            log_many(
                audit_dir=audit_dir,
                records=recs,
                filename=str(getattr(cfg, "audit_changes_filename", "auditoria_cambios.jsonl")),
                rotate_max_bytes=int(getattr(cfg, "audit_rotate_max_bytes", 0) or 0),
            )
    except Exception:
        pass

    return {"bundle": bundle_path, "index": index_path, "latest": latest_path}


def verificar_auditoria_bundle(bundle_path: Path, script_dir: Path, cfg: Any) -> bool:
    """Verifica la firma del bundle (si está habilitada)."""

    try:
        obj = json.loads(Path(bundle_path).read_text(encoding="utf-8"))
        algo = str(obj.get("signature_algo") or "")
        if algo != "HMAC-SHA256":
            return False
        signature = str(obj.get("signature") or "")
        payload_obj = {"meta": obj.get("meta"), "entries": obj.get("entries")}
        payload_bytes = json.dumps(
            payload_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        key_dir = _resolve_audit_key_dir(script_dir, cfg)
        key_hex, _kid = get_or_create_audit_key(
            script_dir,
            filename=str(getattr(cfg, "audit_key_filename", "audit_key.txt")),
            key_dir=key_dir,
            appname=str(getattr(cfg, "app_name", "procesador") or "procesador"),
        )
        return verify_hmac_sha256_hex(key_hex, payload_bytes, signature)
    except Exception:
        return False


def _fmt_hhmm(t: Optional[time]) -> str:
    if t is None:
        return ""
    return f"{t.hour:02d}:{t.minute:02d}"


def _minutes_to_hhmm(m: int) -> str:
    sign = "-" if m < 0 else ""
    m = abs(int(m))
    return f"{sign}{m // 60:02d}:{m % 60:02d}"


def _minutes_to_hhmm_with_min(m: int) -> str:
    """Formato HH:MM + minutos (para transparencia en dashboard)."""
    m = int(m or 0)
    return f"{_minutes_to_hhmm(m)} ({m} min)"


def _safe_input(prompt: str, default: str = "") -> str:
    try:
        s = input(prompt)
    except EOFError:
        return default
    s = (s or "").strip()
    return s if s else default


def _is_yes(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"s", "si", "sí", "y", "yes"}


def _sanitize_text(s: str, max_len: int = 300) -> str:
    # Evita saltos de línea y control chars para logs/JSON.
    s = (s or "").replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _validar_hhmm(s: str) -> Optional[time]:
    return _legacy().parse_time(s)


def _normalize_times(times: List[time], modo_seguro: bool) -> Tuple[List[time], bool]:
    if modo_seguro:
        return times[:], False
    return _legacy().normalize_registro_times(times)


def _map_eventos(times_norm: List[time]) -> Dict[str, Optional[time]]:
    return _legacy().map_eventos(times_norm)


def _recalc_preview(
    eventos: Dict[str, Optional[time]],
    cfg: Any,
    no_labor: Optional[List[Tuple[Optional[time], Optional[time], str]]] = None,
) -> Dict[str, object]:
    trabajado, extra, comida_ded, cena_ded, no_lab_ded, nolab_overlap_cd, nolab_sol_int, ignored_out = (
        _legacy().calcular_trabajado(eventos, cfg, no_laborado_extra=no_labor)
    )

    # Datos extra solo para UI (dashboard). No se exporta a Excel.
    umbral = int(getattr(cfg, "umbral_comida_media_hora_min", 60) or 60)
    tope = int(getattr(cfg, "tope_descuento_comida_min", 30) or 30)
    comida_real, _comida_desc, _comida_motivo = _legacy().calcular_descuento_comida(eventos, cfg)
    cena_real, _cena_desc, _cena_motivo = _legacy().calcular_descuento_cena(eventos, cfg)

    # Intervalos para mostrar en dashboard (si existen)
    comida_a = eventos.get("Salida a comer")
    comida_b = eventos.get("Regreso de comer")
    cena_a = eventos.get("Salida a cenar")
    cena_b = eventos.get("Regreso de cenar")
    comida_interval = f"({_fmt_hhmm(comida_a)} → {_fmt_hhmm(comida_b)})" if (comida_a and comida_b) else ""
    cena_interval = f"({_fmt_hhmm(cena_a)} → {_fmt_hhmm(cena_b)})" if (cena_a and cena_b) else ""

    # Mensaje humano (lo que RH entiende)
    if comida_real <= 0 or comida_ded <= 0:
        comida_explain = "(sin comida)"
    else:
        if comida_real <= umbral:
            comida_explain = f"(<= {_minutes_to_hhmm(umbral)} → descuenta {_minutes_to_hhmm(min(tope, comida_real))})"
        else:
            comida_explain = f"(> {_minutes_to_hhmm(umbral)}, completo)"

    if cena_real <= 0 or cena_ded <= 0:
        cena_explain = "(sin cena)"
    else:
        cena_explain = "(completo)"

    return {
        "trab_min": trabajado,
        "extra_min": extra,
        "trab_hhmm": _minutes_to_hhmm(trabajado),
        "extra_hhmm": _minutes_to_hhmm(extra),
        # Descuentos en minutos + HH:MM para UI
        "comida_min": comida_ded,
        "cena_min": cena_ded,
        "nolabor_min": no_lab_ded,
        "comida_desc": _minutes_to_hhmm(comida_ded),
        "cena_desc": _minutes_to_hhmm(cena_ded),
        # Duraciones reales (para transparencia)
        "comida_real_min": comida_real,
        "cena_real_min": cena_real,
        "comida_real_hhmm": _minutes_to_hhmm(comida_real),
        "cena_real_hhmm": _minutes_to_hhmm(cena_real),
        "comida_interval": comida_interval,
        "cena_interval": cena_interval,
        # Históricamente este campo se llamó extra_desc; en realidad es el descuento por NoLaborado.
        "extra_desc": _minutes_to_hhmm(no_lab_ded),
        "nolabor_desc": _minutes_to_hhmm(no_lab_ded),
        "nolab_overlap_cd": nolab_overlap_cd,
        "nolab_sol_int": nolab_sol_int,
        "ignored_out": ignored_out,
        # Explicación RH
        "comida_explain": comida_explain,
        "cena_explain": cena_explain,
        # Solo para UI en consola (no se exporta a Excel).
        "_no_labor_list": no_labor or [],
    }


def _validate_times(times: List[time], times_norm: List[time]) -> Tuple[List[str], List[str]]:
    """Valida la lista de checadas (best-effort).

    Returns (warnings, errors)."""

    warn: List[str] = []
    err: List[str] = []

    if not times:
        warn.append("Sin checadas")
        return warn, err

    if len(times) == 1:
        warn.append("Solo 1 checada (entrada/salida incompleta)")

    if len(times) > 6:
        warn.append(f"Más de 6 checadas ({len(times)}); se mapearán solo las primeras 6")

    # Duplicados exactos
    seen = set()
    dups = []
    for t in times:
        k = (t.hour, t.minute)
        if k in seen:
            dups.append(_fmt_hhmm(t))
        seen.add(k)
    if dups:
        warn.append(f"Duplicados detectados: {', '.join(sorted(set(dups)))}")

    # Cruce de medianoche
    if times_norm and len(times_norm) >= 2:
        if times_norm[-1] < times_norm[0]:
            warn.append("Posible cruce de medianoche (Salida < Entrada)")

    # Orden sospechoso: si normalizado difiere mucho
    if len(times) >= 3 and times != times_norm:
        warn.append("Normalización reordenó checadas")

    # Rango
    try:
        for t in times:
            if t.hour < 0 or t.hour > 23 or t.minute < 0 or t.minute > 59:
                err.append("Hora fuera de rango")
                break
    except Exception:
        err.append("Hora inválida")

    return warn, err




def _safe_print_menu_line(line: str) -> None:
    """Imprime una línea del menú tolerando consolas con encoding limitado (Windows)."""
    try:
        print(line)
    except UnicodeEncodeError:
        import unicodedata
        safe = unicodedata.normalize("NFKD", line).encode("ascii", "ignore").decode("ascii")
        print(safe)

def mostrar_menu_admin() -> None:
    """Muestra el menú principal del dashboard (modo admin).

    Solo presentación (prints). No modifica lógica de negocio ni comportamiento
    de las opciones: los números y las acciones permanecen iguales.
    """
    _safe_print_menu_line("==========================================================================================")
    _safe_print_menu_line("DASHBOARD / EDITOR (modo admin)")
    _safe_print_menu_line("------------------------------------------------------------------------------------------")

    _safe_print_menu_line("[ADMINISTRACIÓN]")
    _safe_print_menu_line("10) Administrar grupos / IDGRUPO / Activos    (asignar faltantes, mover, bajas/activos)")
    _safe_print_menu_line("")

    _safe_print_menu_line("[EDICIÓN RÁPIDA — recomendado]")
    _safe_print_menu_line(" 1) Editar eventos mapeados                   (Entrada / Comer / Cena / Salida)")
    _safe_print_menu_line(" 8) Permiso NoLaborado                        (agregar/editar intervalos)")
    _safe_print_menu_line(" 7) Aplicar estos cambios a múltiples días    (BULK)")
    _safe_print_menu_line("")

    _safe_print_menu_line("[CONTROL Y VALIDACIÓN]")
    _safe_print_menu_line(" 4) Recalcular y validar                      (ver si cuadra todo)")
    _safe_print_menu_line(" 5) Revertir cambios                          (volver al estado original)")
    _safe_print_menu_line(" 6) Guardar y continuar")
    _safe_print_menu_line("")

    _safe_print_menu_line("[AVANZADO — usar solo si hace falta]")
    _safe_print_menu_line(" 2) Insertar marca manual                     (agrega una checada)")
    _safe_print_menu_line(" 9) Editar una marca específica               (editar una checada puntual)")
    _safe_print_menu_line(" 3) Borrar una marca                          (eliminar una checada)")
    _safe_print_menu_line("")

    _safe_print_menu_line("[SALIR]")
    _safe_print_menu_line(" 0) Salir sin guardar                         (volver al menú anterior)")
    _safe_print_menu_line("------------------------------------------------------------------------------------------")


def _render_dashboard(
    *,
    emp_id: str,
    nombre: str,
    fecha_d: date,
    registro_raw: str,
    times: List[time],
    times_norm: List[time],
    eventos: Dict[str, Optional[time]],
    preview: Dict[str, object],
    dirty: bool,
    notas: str,
    bulk_plan: Optional[Dict[str, object]],
) -> None:
    print("\n" + "=" * 90)
    print(f"EDICIÓN CHECADAS | ID: {emp_id} | Nombre: {nombre} | Fecha: {fecha_d.isoformat()}")
    print("-" * 90)
    print(f"Registro original: {registro_raw}")
    print(f"Parseadas:    {[ _fmt_hhmm(t) for t in times ]}")
    print(f"Normalizadas: {[ _fmt_hhmm(t) for t in times_norm ]}")

    # Vista D / D+1 para evitar confusión en turnos que cruzan medianoche

    day_offsets: List[int] = []

    day = 0

    prev_key = None

    for _t in times_norm:

        key = (_t.hour, _t.minute)

        if prev_key is not None and key < prev_key:

            day += 1

        day_offsets.append(day)

        prev_key = key


    def _tag(idx: int) -> str:

        d = day_offsets[idx] if 0 <= idx < len(day_offsets) else 0

        return ' (D)' if d == 0 else f' (D+{d})'


    def _tag_for_time(t: Optional[time]) -> str:

        if t is None:

            return ''

        for i, _t in enumerate(times_norm):

            if (_t.hour, _t.minute) == (t.hour, t.minute):

                return _tag(i)

        return ''


    if any(d > 0 for d in day_offsets):

        norm_dd1 = [f'{_fmt_hhmm(times_norm[i])}{_tag(i)}' for i in range(len(times_norm))]

        print(f"Normalizadas (D/D+1): {norm_dd1}")
    print("Eventos mapeados:")
    for k in [
        "Entrada",
        "Salida a comer",
        "Regreso de comer",
        "Salida a cenar",
        "Regreso de cenar",
        "Salida",
    ]:
        print(f" - {k:16}: {_fmt_hhmm(eventos.get(k))}{_tag_for_time(eventos.get(k))}")

    # Checadas extra/no usadas (cuando hay más de 6)
    if len(times_norm) > 6:
        extras = [f"{_fmt_hhmm(times_norm[i])}{_tag(i)}" for i in range(6, len(times_norm))]
        if extras:
            print(f"Checadas no usadas: {extras}")
    print("-" * 90)
    print(
        f"Trabajado provisional: {preview.get('trab_hhmm')} | Extra: {preview.get('extra_hhmm')} "
        f"| Desc comida: {preview.get('comida_desc')} | Desc cena: {preview.get('cena_desc')} | Desc NoLaborado: {preview.get('nolabor_desc', preview.get('extra_desc'))}"
    )
    # Transparencia para RRHH: por qué se descuenta así
    # Duración real (lo que realmente se tardó) + descuento aplicado
    comida_real = int(preview.get('comida_real_min') or 0)
    cena_real = int(preview.get('cena_real_min') or 0)
    if comida_real > 0:
        print(
            f"Comida real:      {_minutes_to_hhmm_with_min(comida_real)} {preview.get('comida_interval', '')}".rstrip()
        )
    else:
        print("Comida real:      (sin comida)")

    print(f"Descuento comida: {preview.get('comida_desc')} {preview.get('comida_explain')}")

    if cena_real > 0:
        print(
            f"Cena real:        {_minutes_to_hhmm_with_min(cena_real)} {preview.get('cena_interval', '')}".rstrip()
        )
    else:
        print("Cena real:        (sin cena)")

    print(f"Descuento cena:   {preview.get('cena_desc')} {preview.get('cena_explain')}")
    nol_txt = _fmt_nolabor(preview.get('_no_labor_list')) if isinstance(preview.get('_no_labor_list'), list) else ""
    if nol_txt:
        print(f"NoLaborado (intervalos): {nol_txt}")
    if notas:
        print(f"Notas: {notas}")
    if bulk_plan:
        n = len(bulk_plan.get("dates", []) or [])
        print(f"BULK pendiente: se aplicará a {n} fecha(s) al continuar el lote")
    print("Estado:", "CAMBIOS PENDIENTES" if dirty else "SIN CAMBIOS")
    print("=" * 90)
    mostrar_menu_admin()
    print("")
def _fmt_nolabor(no_labor: Optional[List[Tuple[Optional[time], Optional[time], str]]]) -> str:
    """Formatea intervalos NoLaborado a un string compacto."""
    if not no_labor:
        return ""
    parts: List[str] = []
    for a, b, motivo in no_labor:
        sa = _fmt_hhmm(a)
        sb = _fmt_hhmm(b)
        m = (motivo or "").strip()
        if m:
            parts.append(f"{sa}-{sb} ({m})")
        else:
            parts.append(f"{sa}-{sb}")
    return "; ".join(parts)


def _parse_hhmm_strict(txt: str) -> Optional[time]:
    """HH:MM estricto (00:00–23:59)."""
    t = _legacy().parse_time(txt)
    return t


def _edit_nolabor_interactivo(
    *,
    run_id: str,
    emp_id: str,
    fecha_d: date,
    usuario: str,
    audit_log: List[AuditEntry],
    no_labor: List[Tuple[Optional[time], Optional[time], str]],
) -> List[Tuple[Optional[time], Optional[time], str]]:
    """Submenú para editar intervalos NoLaborado."""

    def _audit(action: str, campo: str, antes: object, despues: object, motivo: str) -> None:
        audit_log.append(
            AuditEntry(
                run_id=str(run_id),
                emp_id=str(emp_id),
                fecha=fecha_d.isoformat(),
                usuario=str(usuario or "RRHH"),
                ts=datetime.now().isoformat(timespec="seconds"),
                accion=str(action),
                campo=str(campo),
                antes=antes,
                despues=despues,
                motivo=str(motivo or ""),
            )
        )

    while True:
        print("\n--- PERMISO NO LABORADO ---")
        if not no_labor:
            print("(sin intervalos)")
        else:
            for i, (a, b, m) in enumerate(no_labor, start=1):
                sa, sb = _fmt_hhmm(a), _fmt_hhmm(b)
                mm = (m or "").strip()
                print(f" {i}) {sa}-{sb}" + (f" | {mm}" if mm else ""))
        print("Opciones NoLaborado:")
        print(" 1) Agregar intervalo")
        print(" 2) Editar intervalo")
        print(" 3) Borrar intervalo")
        print(" 4) Borrar TODOS")
        print(" 0) Volver")
        op = _safe_input("> ", "0").strip()
        if op == "0":
            return no_labor

        if op == "4":
            if no_labor:
                before = _fmt_nolabor(no_labor)
                motivo = _safe_input("Motivo: ", "").strip()
                no_labor = []
                _audit("NOLAB_CLEAR", "no_labor", before, "", motivo)
            continue

        if op in ("2", "3"):
            if not no_labor:
                print("No hay intervalos para modificar.")
                continue
            idx_txt = _safe_input(f"Índice (1..{len(no_labor)}): ", "").strip()
            if not idx_txt.isdigit():
                print("Índice inválido.")
                continue
            idx = int(idx_txt) - 1
            if not (0 <= idx < len(no_labor)):
                print("Fuera de rango.")
                continue

            if op == "3":
                before = no_labor[idx]
                motivo = _safe_input("Motivo: ", "").strip()
                del no_labor[idx]
                _audit("NOLAB_DELETE", f"no_labor[{idx}]", _fmt_nolabor([before]), "", motivo)
                continue

            # editar
            a0, b0, m0 = no_labor[idx]
            print(f"Actual: {_fmt_hhmm(a0)}-{_fmt_hhmm(b0)} | {(m0 or '').strip()}")
            a_txt = _safe_input("Inicio (HH:MM): ", _fmt_hhmm(a0)).strip()
            b_txt = _safe_input("Fin (HH:MM): ", _fmt_hhmm(b0)).strip()
            a = _parse_hhmm_strict(a_txt)
            b = _parse_hhmm_strict(b_txt)
            if not a or not b:
                print("Hora inválida. Usa HH:MM.")
                continue
            if a == b:
                print("Inicio y fin no pueden ser iguales.")
                continue
            if b < a:
                print("AVISO: fin < inicio. Se interpretará como cruce de medianoche.")
            motivo = _safe_input("Motivo (obligatorio): ", "").strip()
            if not motivo:
                print("Motivo requerido.")
                continue
            new_m = _safe_input("Etiqueta/nota (opcional): ", (m0 or "")).strip()
            before = no_labor[idx]
            no_labor[idx] = (a, b, new_m)
            _audit("NOLAB_EDIT", f"no_labor[{idx}]", _fmt_nolabor([before]), _fmt_nolabor([no_labor[idx]]), motivo)
            continue

        if op == "1":
            a_txt = _safe_input("Inicio (HH:MM): ", "").strip()
            b_txt = _safe_input("Fin (HH:MM): ", "").strip()
            a = _parse_hhmm_strict(a_txt)
            b = _parse_hhmm_strict(b_txt)
            if not a or not b:
                print("Hora inválida. Usa HH:MM.")
                continue
            if a == b:
                print("Inicio y fin no pueden ser iguales.")
                continue
            if b < a:
                print("AVISO: fin < inicio. Se interpretará como cruce de medianoche.")
            motivo = _safe_input("Motivo (obligatorio): ", "").strip()
            if not motivo:
                print("Motivo requerido.")
                continue
            etiqueta = _safe_input("Etiqueta/nota (opcional): ", "").strip()
            new = (a, b, etiqueta)
            no_labor.append(new)
            _audit("NOLAB_INSERT", f"no_labor[{len(no_labor)-1}]", "", _fmt_nolabor([new]), motivo)
            continue

        print("Opción inválida.")


def _parse_date_list(expr: str) -> List[date]:
    """Parsea una lista/rango de fechas.

    Acepta:
      - 2026-01-01,2026-01-02
      - 2026-01-01..2026-01-07
      - mezcla con comas
    """

    expr = (expr or "").strip()
    if not expr:
        return []
    parts = [p.strip() for p in expr.split(",") if p.strip()]
    out: List[date] = []
    for p in parts:
        if ".." in p:
            a, b = [x.strip() for x in p.split("..", 1)]
            da = _legacy().parse_date(a)
            db = _legacy().parse_date(b)
            if not da or not db:
                continue
            if db < da:
                da, db = db, da
            cur = da
            while cur <= db:
                out.append(cur)
                cur = cur.fromordinal(cur.toordinal() + 1)
        else:
            d = _legacy().parse_date(p)
            if d:
                out.append(d)
    # uniq preserve order
    seen = set()
    final = []
    for d in out:
        if d in seen:
            continue
        seen.add(d)
        final.append(d)
    return final


def editar_checadas_interactivo(
    *,
    run_id: str,
    emp_id: str,
    nombre: str,
    fecha_d: date,
    registro_raw: str,
    registro_display: str | None = None,
    cfg: Any,
    usuario: str,
    audit_log: List[AuditEntry],
    modo_seguro: bool = False,
    script_dir: 'Path | None' = None,
    processed_ids: 'list[str] | None' = None,
    no_labor: Optional[List[Tuple[Optional[time], Optional[time], str]]] = None,
) -> Tuple[Optional[List[time]], str, Optional[Dict[str, object]], List[Tuple[Optional[time], Optional[time], str]]]:
    """Interactive editor for one (employee, date) record.

    Returns:
        (times_edit or None, nota_final, bulk_plan, no_labor_out)

    - times_edit is None if user exits without saving.
    - bulk_plan (if not None) indicates to apply the FINAL times to other dates for same emp_id.
    - no_labor_out is the list of NoLaborado intervals to apply for (emp_id, fecha_d).
    """

    audit_log = audit_log if audit_log is not None else []

    def _audit(accion: str, campo: str, antes: object, despues: object, motivo: str) -> None:
        audit_log.append(
            AuditEntry(
                run_id=str(run_id),
                emp_id=str(emp_id),
                fecha=fecha_d.isoformat(),
                usuario=usuario or "RRHH",
                ts=datetime.now().isoformat(timespec="seconds"),
                accion=accion,
                campo=campo,
                antes=antes,
                despues=despues,
                motivo=_sanitize_text(motivo or ""),
            )
        )

    # registro_display solo es visual; registro_raw es lo que se parsea
    if registro_display is None:
        registro_display = registro_raw

    times = _legacy().parse_registro(registro_raw)
    times_snapshot = copy.deepcopy(times)

    no_labor_list: List[Tuple[Optional[time], Optional[time], str]] = []
    if isinstance(no_labor, list):
        # Copia defensiva: solo se persiste si el usuario guarda.
        no_labor_list = copy.deepcopy(no_labor)
    no_labor_snapshot = copy.deepcopy(no_labor_list)

    nota_final = ""
    dirty = False
    bulk_plan: Optional[Dict[str, object]] = None

    while True:
        times_norm, reord = _normalize_times(times, modo_seguro)
        eventos = _map_eventos(times_norm)
        preview = _recalc_preview(eventos, cfg, no_labor=no_labor_list)

        warn, err = _validate_times(times, times_norm)
        notas = ""
        if reord and not modo_seguro:
            notas += "Normalización reordenó checadas. "
        if warn:
            notas += "WARN: " + "; ".join(warn) + ". "
        if err:
            notas += "ERROR: " + "; ".join(err) + ". "

        _render_dashboard(
            emp_id=emp_id,
            nombre=nombre,
            fecha_d=fecha_d,
            registro_raw=registro_display,
            times=times,
            times_norm=times_norm,
            eventos=eventos,
            preview=preview,
            dirty=dirty,
            notas=notas.strip(),
            bulk_plan=bulk_plan,
        )

        op = _safe_input("Opción: ").strip()
        if op == "0":
            # salir SIN guardar: descartar cambios de checadas y NoLaborado
            return None, "", None, no_labor_snapshot

        if op == "10":
            # Administración de grupos/IDGRUPO/Activos (para faltas). No modifica checadas.
            try:
                if script_dir is None:
                    # best-effort: carpeta del paquete
                    script_dir = Path(__file__).resolve().parent
                run_group_admin(
                    script_dir=Path(script_dir),
                    cfg=cfg,
                    processed_ids=processed_ids or [],
                    usuario=usuario or "RRHH",
                )
            except Exception as e:
                # No silencios: mostramos algo mínimo y dejamos rastro en logs
                try:
                    from .logger import log_exception
                    log_exception("Fallo en admin de grupos (dashboard)", extra={"emp_id": str(emp_id), "fecha": str(fecha_d)})
                except Exception:
                    pass
                print(f"\n[WARN] No se pudo abrir el admin de grupos: {e}")
            continue

        if op == "1":
            # Edición por EVENTO mapeado (recomendado). Esto evita que RH tenga que
            # entender la lista cruda de checadas.
            event_order = [
                "Entrada",
                "Salida a comer",
                "Regreso de comer",
                "Salida a cenar",
                "Regreso de cenar",
                "Salida",
            ]
            print("\nEditar evento mapeado:")
            for i, k in enumerate(event_order, start=1):
                v = eventos.get(k)
                tag = ""  # etiqueta D/D+1 ya se muestra arriba
                missing = " (FALTA)" if v is None else ""
                print(f" {i}) {k:16}: {_fmt_hhmm(v)}{tag}{missing}")
            idx_s = _safe_input("Número de evento (1..6) o 0 para volver: ").strip()
            if idx_s in ("0", "", "b", "B"):
                continue
            if not idx_s.isdigit():
                print("Índice inválido.")
                continue
            eidx = int(idx_s) - 1
            if eidx < 0 or eidx >= len(event_order):
                print("Fuera de rango.")
                continue

            k = event_order[eidx]
            before_t = eventos.get(k)
            new_s = _safe_input("Nueva hora (HH:MM): ").strip()
            tnew = _validar_hhmm(new_s)
            if not tnew:
                print("Hora inválida.")
                continue
            motivo = _safe_input("Motivo (obligatorio): ").strip()
            if not motivo:
                print("Motivo requerido.")
                continue

            # Construye slots fijos (6) desde eventos actuales.
            core_slots: List[Optional[time]] = [eventos.get(name) for name in event_order]
            unused: List[time] = times_norm[6:] if len(times_norm) > 6 else []

            core_slots[eidx] = tnew

            # Reconstruye la lista de checadas: slots (sin None) + checadas no usadas.
            new_times = [t for t in core_slots if t is not None] + list(unused)
            times = new_times
            dirty = True
            action = "SET_EVENT" if before_t is None else "EDIT_EVENT"
            _audit(
                action,
                f"eventos.{k}",
                _fmt_hhmm(before_t) if before_t else None,
                _fmt_hhmm(tnew),
                motivo,
            )

        elif op == "2":
            new_s = _safe_input("Hora a insertar (HH:MM) o 0 para volver: ").strip()
            if new_s in ("0", "", "b", "B"):
                continue
            tnew = _validar_hhmm(new_s)
            if not tnew:
                print("Hora inválida.")
                continue
            pos_s = _safe_input(f"Posición (1..{len(times)+1}) [Enter=final]: ").strip()
            pos = len(times)
            if pos_s:
                if not pos_s.isdigit():
                    print("Posición inválida.")
                    continue
                pos = int(pos_s) - 1
                if pos < 0 or pos > len(times):
                    print("Fuera de rango.")
                    continue
            motivo = _safe_input("Motivo: ").strip()
            times.insert(pos, tnew)
            dirty = True
            _audit("INSERT", f"times[{pos}]", None, _fmt_hhmm(tnew), motivo)

        elif op == "3":
            if not times:
                print("No hay checadas para borrar.")
                continue
            for i, t in enumerate(times, start=1):
                print(f" {i}) {_fmt_hhmm(t)}")
            idx_s = _safe_input("Número a borrar o 0 para volver: ").strip()
            if idx_s in ("0", "", "b", "B"):
                continue
            if not idx_s.isdigit():
                print("Índice inválido.")
                continue
            idx = int(idx_s) - 1
            if idx < 0 or idx >= len(times):
                print("Fuera de rango.")
                continue
            motivo = _safe_input("Motivo: ").strip()
            antes = _fmt_hhmm(times[idx])
            del times[idx]
            dirty = True
            _audit("DELETE", f"times[{idx}]", antes, None, motivo)

        elif op == "9":
            # Edición de checadas crudas (modo avanzado)
            if not times:
                print("No hay checadas para editar.")
                continue
            for i, t in enumerate(times, start=1):
                print(f" {i}) {_fmt_hhmm(t)}")
            idx_s = _safe_input("Número a editar o 0 para volver: ").strip()
            if idx_s in ("0", "", "b", "B"):
                continue
            if not idx_s.isdigit():
                print("Índice inválido.")
                continue
            idx = int(idx_s) - 1
            if idx < 0 or idx >= len(times):
                print("Fuera de rango.")
                continue
            new_s = _safe_input("Nueva hora (HH:MM): ").strip()
            tnew = _validar_hhmm(new_s)
            if not tnew:
                print("Hora inválida.")
                continue
            motivo = _safe_input("Motivo (obligatorio): ").strip()
            if not motivo:
                print("Motivo requerido.")
                continue
            antes = _fmt_hhmm(times[idx])
            times[idx] = tnew
            dirty = True
            _audit("EDIT", f"times[{idx}]", antes, _fmt_hhmm(tnew), motivo)

        elif op == "4":
            print("Recalculado.")
            continue

        elif op == "5":
            motivo = _safe_input("Motivo de revertir: ").strip()
            _audit("REVERT", "state", "dirty", "snapshot", motivo)
            times = copy.deepcopy(times_snapshot)
            no_labor_list = copy.deepcopy(no_labor_snapshot)
            dirty = False
            bulk_plan = None

        elif op == "8":
            # Editor de permisos NoLaborado (pocos casos, se justifica interacción)
            before = _fmt_nolabor(no_labor_list)
            no_labor_list = _edit_nolabor_interactivo(
                run_id=run_id,
                emp_id=emp_id,
                fecha_d=fecha_d,
                usuario=usuario,
                audit_log=audit_log,
                no_labor=no_labor_list,
            )
            after = _fmt_nolabor(no_labor_list)
            if after != before:
                dirty = True

        elif op == "7":
            if not dirty:
                print("Primero realiza los cambios (editar/insertar/borrar) antes de BULK.")
                continue
            expr = _safe_input(
                "Fechas destino (YYYY-MM-DD, separadas por coma, o rango A..B) o 0 para volver: ",
                "",
            )
            expr = (expr or "").strip()
            if expr in ("0", "", "b", "B"):
                continue
            fechas = _parse_date_list(expr)
            # nunca incluir la fecha actual (ya se está editando)
            fechas = [d for d in fechas if d != fecha_d]
            if not fechas:
                print("No se detectaron fechas válidas.")
                continue
            motivo = _safe_input("Motivo BULK (se registrará en auditoría): ").strip()
            # Replicar el estado FINAL que el usuario ve (normalizado),
            # para evitar aplicar listas fuera de orden en cruces de medianoche.
            times_norm, _ = _normalize_times(times, modo_seguro)
            bulk_plan = {
                "emp_id": emp_id,
                "source_fecha": fecha_d.isoformat(),
                "dates": [d.isoformat() for d in fechas],
                "times": [_fmt_hhmm(t) for t in times_norm],
                "motivo": _sanitize_text(motivo),
                "usuario": usuario or "RRHH",
            }
            print(f"BULK listo: se aplicará a {len(fechas)} fecha(s) al continuar el lote.")

        elif op == "6":
            # Validación extra antes de guardar
            times_norm, _ = _normalize_times(times, modo_seguro)
            warn, err = _validate_times(times, times_norm)
            if err:
                print("No se puede guardar por errores:")
                for e in err:
                    print(" -", e)
                continue
            if warn and modo_seguro:
                print("Advertencias:")
                for w in warn:
                    print(" -", w)
                ok = _is_yes(_safe_input("¿Guardar de todos modos? (S/N): ", "N"))
                if not ok:
                    continue

            if not dirty:
                return times, "", bulk_plan, no_labor_list
            nota_final = _safe_input("Motivo/nota final de guardado: ").strip()
            _audit("SAVE", "state", "dirty", "saved", nota_final)
            return times, nota_final, bulk_plan, no_labor_list

        else:
            print("Opción inválida.")


def construir_reporte_auditoria(*, run_meta: Dict[str, object], audit_log: List[AuditEntry]) -> Dict[str, pd.DataFrame]:
    """Construye DataFrames para un reporte de auditoría."""

    run_rows = [{"Campo": k, "Valor": v} for k, v in (run_meta or {}).items()]
    df_run = pd.DataFrame(run_rows)

    if audit_log:
        df_ed = pd.DataFrame([asdict(a) for a in audit_log])
    else:
        df_ed = pd.DataFrame(
            columns=["run_id", "emp_id", "fecha", "accion", "campo", "antes", "despues", "motivo", "usuario", "ts"]
        )

    for col in ["emp_id", "fecha", "accion", "usuario", "campo"]:
        if col in df_ed.columns:
            df_ed[col] = df_ed[col].astype(str)

    def _group_count(col: str, name: str) -> pd.DataFrame:
        if col not in df_ed.columns or df_ed.empty:
            return pd.DataFrame(columns=[name, "ediciones"])
        s = df_ed[col].fillna("").astype(str)
        out = s.value_counts().reset_index()
        out.columns = [name, "ediciones"]
        return out

    return {
        "RUN": df_run,
        "EDICIONES": df_ed,
        "POR_USUARIO": _group_count("usuario", "usuario"),
        "POR_EMPLEADO": _group_count("emp_id", "emp_id"),
        "POR_FECHA": _group_count("fecha", "fecha"),
        "POR_ACCION": _group_count("accion", "accion"),
    }
