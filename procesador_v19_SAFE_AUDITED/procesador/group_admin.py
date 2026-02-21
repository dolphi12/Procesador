"""Dashboard: administración de grupos / IDGRUPO / estatus (activo/baja).

Este módulo implementa un menú interactivo pensado para RH:
- Detecta empleados procesados sin IDGRUPO o sin grupo asignado
- Permite asignar/bulk asignar grupo e IDGRUPO
- Permite mover empleados entre grupos
- Permite eliminar empleados del mapa
- Permite eliminar grupos (con reubicación conservadora)
- Permite marcar empleados como Activo o Baja (para cálculo de faltas)
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from .config import AppConfig, guardar_config
from .audit import log_change
from .utils import _coerce_id_str

def _safe_input(prompt: str, default: str = "") -> str:
    try:
        v = input(prompt)
    except EOFError:
        return default
    v = (v or "").strip()
    return v if v else default

def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _is_valid_group(g: str, cfg: AppConfig) -> bool:
    g = (g or "").strip()
    return bool(g) and g in (cfg.grupos_orden or [])

def _ensure_group_exists(g: str, cfg: AppConfig) -> None:
    g = (g or "").strip()
    if not g:
        return
    if g not in cfg.grupos_orden:
        cfg.grupos_orden.append(g)
    if g not in (cfg.grupos_meta or {}):
        cfg.grupos_meta[g] = {"prefijo": g}

def _suggest_idgrupo(emp_id: str, cfg: AppConfig) -> str:
    emp = (emp_id or "").strip()
    if not emp or emp.startswith("NOMBRE::"):
        return ""
    g = (cfg.empleado_a_grupo or {}).get(emp, (cfg.grupos_orden or ["000"])[0])
    pref = cfg.prefijo_de_grupo(g)
    return f"{pref}-{emp}"

def _audit_cfg(
    *,
    script_dir: Path,
    cfg: AppConfig,
    usuario: str,
    accion: str,
    detalle: Dict[str, object],
    motivo: str = "",
) -> None:
    audit_dir = Path(script_dir) / (cfg.audit_dir_name or "auditoria")
    rec = {
        "ts": _now(),
        "usuario": usuario or "RRHH",
        "accion": accion,
        "motivo": motivo or "",
        **{f"d_{k}": v for k, v in (detalle or {}).items()},
    }
    log_change(
        audit_dir=audit_dir,
        record=rec,
        filename=cfg.audit_changes_filename or "auditoria_cambios.jsonl",
        rotate_max_bytes=int(getattr(cfg, "audit_rotate_max_bytes", 0) or 0),
    )

def _list_missing(processed_ids: Iterable[str], cfg: AppConfig) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for emp in processed_ids or []:
        k = (emp or "").strip()
        if not k or k.startswith("NOMBRE::"):
            continue
        if not (cfg.empleado_a_grupo or {}).get(k):
            out[k] = "SIN_GRUPO"
            continue
        if not (cfg.empleado_a_idgrupo or {}).get(k):
            out[k] = "SIN_IDGRUPO"
            continue
    return out

def _print_groups(cfg: AppConfig) -> None:
    print("\nGrupos disponibles (orden):")
    for i, g in enumerate(cfg.grupos_orden or [], start=1):
        pref = (cfg.grupos_meta or {}).get(g, {}).get("prefijo", g)
        print(f"  {i:>2}) {g}  (prefijo={pref})")

def _select_group(cfg: AppConfig) -> str:
    _print_groups(cfg)
    g = _safe_input("Grupo (exacto) [Enter=primer grupo]: ", "")
    if not g:
        return (cfg.grupos_orden or ["000"])[0]
    if _is_valid_group(g, cfg):
        return g
    resp = _safe_input(f"Grupo '{g}' no existe. ¿Crear? (S/N): ", "N").strip().upper()
    if resp == "S":
        _ensure_group_exists(g, cfg)
        return g
    return (cfg.grupos_orden or ["000"])[0]

def _select_employee(processed_ids: Iterable[str], cfg: AppConfig) -> str:
    ids = [str(x).strip() for x in (processed_ids or []) if str(x).strip()]
    ids = [x for x in ids if not x.startswith("NOMBRE::")]
    ids = sorted(set(ids), key=lambda x: x)
    if not ids:
        return ""
    print("\nEmpleados detectados (ID):")
    for i, emp in enumerate(ids, start=1):
        g = (cfg.empleado_a_grupo or {}).get(emp, "")
        ig = (cfg.empleado_a_idgrupo or {}).get(emp, "")
        st = (cfg.empleado_status or {}).get(emp, {}).get("activo", True)
        st_txt = "ACTIVO" if bool(st) else "BAJA"
        print(f"  {i:>3}) {emp} | grp={g or '-'} | idgrupo={ig or '-'} | {st_txt}")
    sel = _safe_input("Selecciona número (Enter cancela): ", "")
    if not sel:
        return ""
    try:
        idx = int(sel)
        if 1 <= idx <= len(ids):
            return ids[idx - 1]
    except Exception:
        pass
    # permitir teclear ID directo
    if sel.strip() in ids:
        return sel.strip()
    return ""

def _bulk_assign_missing(processed_ids: Iterable[str], cfg: AppConfig, script_dir: Path, usuario: str) -> None:
    missing = _list_missing(processed_ids, cfg)
    if not missing:
        print("\nNo hay empleados sin mapeo (grupo/IDGRUPO).")
        return
    print("\nEmpleados SIN mapa (grupo/IDGRUPO):")
    for emp, reason in missing.items():
        print(f" - {emp} ({reason})")

    print("\nOpciones de asignación:")
    print(" 1) Asignar 1 por 1")
    print(" 2) Asignar el MISMO grupo a TODOS los faltantes y generar IDGRUPO sugerido")
    op = _safe_input("Opción: ", "1").strip()

    if op == "2":
        g = _select_group(cfg)
        pref = cfg.prefijo_de_grupo(g)
        motivo = _safe_input("Motivo (opcional): ", "")
        for emp in list(missing.keys()):
            cfg.empleado_a_grupo[emp] = g
            cfg.empleado_a_idgrupo[emp] = _suggest_idgrupo(emp, cfg)
            cfg.empleado_status.setdefault(emp, {"activo": True})
            _audit_cfg(
                script_dir=script_dir,
                cfg=cfg,
                usuario=usuario,
                accion="MAPA_BULK_SET",
                detalle={"emp_id": emp, "grupo": g, "idgrupo": cfg.empleado_a_idgrupo.get(emp, ""), "prefijo": pref},
                motivo=motivo,
            )
        print(f"\nAsignado grupo '{g}' a {len(missing)} empleado(s).")
        return

    # 1 por 1
    for emp in list(missing.keys()):
        print(f"\n--- Asignación para ID={emp} ---")
        g = _select_group(cfg)
        cfg.empleado_a_grupo[emp] = g
        suger = _suggest_idgrupo(emp, cfg)
        ig = _safe_input(f"IDGRUPO (Enter='{suger}'): ", "")
        cfg.empleado_a_idgrupo[emp] = ig.strip() if ig.strip() else suger
        cfg.empleado_status.setdefault(emp, {"activo": True})
        motivo = _safe_input("Motivo (opcional): ", "")
        _audit_cfg(
            script_dir=script_dir,
            cfg=cfg,
            usuario=usuario,
            accion="MAPA_SET",
            detalle={"emp_id": emp, "grupo": g, "idgrupo": cfg.empleado_a_idgrupo.get(emp, "")},
            motivo=motivo,
        )

def _move_employee(processed_ids: Iterable[str], cfg: AppConfig, script_dir: Path, usuario: str) -> None:
    emp = _select_employee(processed_ids, cfg)
    if not emp:
        return
    old_g = (cfg.empleado_a_grupo or {}).get(emp, "")
    print(f"\nMover empleado {emp} (grupo actual: {old_g or '-'})")
    g = _select_group(cfg)
    if g == old_g:
        print("No hay cambio.")
        return
    regen = _safe_input("¿Regenerar IDGRUPO sugerido según nuevo grupo? (S/N): ", "S").strip().upper() == "S"
    old_ig = (cfg.empleado_a_idgrupo or {}).get(emp, "")
    cfg.empleado_a_grupo[emp] = g
    if regen:
        cfg.empleado_a_idgrupo[emp] = _suggest_idgrupo(emp, cfg)
    motivo = _safe_input("Motivo (obligatorio): ", "")
    if not motivo:
        motivo = "Cambio de grupo"
    _audit_cfg(
        script_dir=script_dir,
        cfg=cfg,
        usuario=usuario,
        accion="MAPA_MOVE",
        detalle={"emp_id": emp, "grupo_old": old_g, "grupo_new": g, "idgrupo_old": old_ig, "idgrupo_new": cfg.empleado_a_idgrupo.get(emp, "")},
        motivo=motivo,
    )
    print("OK.")

def _delete_employee(processed_ids: Iterable[str], cfg: AppConfig, script_dir: Path, usuario: str) -> None:
    emp = _select_employee(processed_ids, cfg)
    if not emp:
        return
    confirm = _safe_input(f"Eliminar empleado {emp} del mapa (S/N): ", "N").strip().upper()
    if confirm != "S":
        return
    old = {
        "grupo": (cfg.empleado_a_grupo or {}).get(emp, ""),
        "idgrupo": (cfg.empleado_a_idgrupo or {}).get(emp, ""),
        "status": (cfg.empleado_status or {}).get(emp, {}),
    }
    cfg.empleado_a_grupo.pop(emp, None)
    cfg.empleado_a_idgrupo.pop(emp, None)
    cfg.empleado_status.pop(emp, None)
    cfg.empleado_meta.pop(emp, None)
    motivo = _safe_input("Motivo (obligatorio): ", "")
    if not motivo:
        motivo = "Eliminación empleado"
    _audit_cfg(script_dir=script_dir, cfg=cfg, usuario=usuario, accion="MAPA_EMP_DEL", detalle={"emp_id": emp, **old}, motivo=motivo)
    print("OK.")

def _delete_group(cfg: AppConfig, script_dir: Path, usuario: str) -> None:
    _print_groups(cfg)
    g = _safe_input("Grupo a eliminar (exacto): ", "").strip()
    if not g or g not in (cfg.grupos_orden or []):
        print("Grupo inválido.")
        return
    if g == (cfg.grupos_orden or ["000"])[0] and len(cfg.grupos_orden or []) == 1:
        print("No se puede eliminar el único grupo.")
        return
    confirm = _safe_input(f"Eliminar grupo '{g}' (S/N): ", "N").strip().upper()
    if confirm != "S":
        return
    # Reubicación conservadora
    default_g = (cfg.grupos_orden or ["000"])[0]
    affected = [emp for emp, grp in (cfg.empleado_a_grupo or {}).items() if grp == g]
    for emp in affected:
        cfg.empleado_a_grupo[emp] = default_g
        # mantener IDGRUPO sería riesgoso si prefijo cambia; lo vaciamos para reasignar.
        cfg.empleado_a_idgrupo[emp] = ""
    cfg.grupos_orden = [x for x in (cfg.grupos_orden or []) if x != g]
    cfg.grupos_meta.pop(g, None)
    motivo = _safe_input("Motivo (obligatorio): ", "")
    if not motivo:
        motivo = "Eliminación grupo"
    _audit_cfg(script_dir=script_dir, cfg=cfg, usuario=usuario, accion="MAPA_GROUP_DEL", detalle={"grupo": g, "affected": len(affected), "moved_to": default_g}, motivo=motivo)
    print(f"OK. {len(affected)} empleado(s) movidos a '{default_g}' con IDGRUPO vacío.")

def _toggle_status(processed_ids: Iterable[str], cfg: AppConfig, script_dir: Path, usuario: str) -> None:
    emp = _select_employee(processed_ids, cfg)
    if not emp:
        return
    cur = bool((cfg.empleado_status or {}).get(emp, {}).get("activo", True))
    print(f"\nEstatus actual: {'ACTIVO' if cur else 'BAJA'}")
    new = _safe_input("Nuevo estatus (A=Activo, B=Baja): ", "A" if not cur else "B").strip().upper()
    if new not in ("A","B"):
        print("Inválido.")
        return
    nuevo_activo = (new == "A")
    motivo = _safe_input("Motivo (obligatorio): ", "")
    if not motivo:
        motivo = "Cambio estatus"
    rec = cfg.empleado_status.setdefault(emp, {"activo": True})
    rec["activo"] = bool(nuevo_activo)
    if not nuevo_activo:
        rec["fecha_baja"] = _now()
    else:
        rec.pop("fecha_baja", None)
        rec.setdefault("fecha_alta", _now())
    _audit_cfg(script_dir=script_dir, cfg=cfg, usuario=usuario, accion="EMP_STATUS", detalle={"emp_id": emp, "activo": bool(nuevo_activo)}, motivo=motivo)
    print("OK.")

def run_group_admin(
    *,
    script_dir: Path,
    cfg: AppConfig,
    processed_ids: Iterable[str],
    usuario: str,
) -> None:
    """Ejecuta menú de administración y guarda config al salir (best-effort)."""
    script_dir = Path(script_dir)
    while True:
        missing = _list_missing(processed_ids, cfg)
        print("\n" + "="*90)
        print("ADMIN GRUPOS / IDGRUPO / ACTIVOS")
        print(f"Empleados procesados: {len(set([str(x).strip() for x in processed_ids or [] if str(x).strip() and not str(x).startswith('NOMBRE::')]))}")
        print(f"Sin mapa: {len(missing)}")
        print("="*90)
        print("Opciones:")
        print("")
        print("[ASIGNACIÓN RÁPIDA — flujo típico]")
        print(" 1) Asignar grupo/IDGRUPO a faltantes        (detectados en este archivo)")
        print(" 6) Ver grupos                               (orden actual de creación)")
        print("")
        print("[MANTENIMIENTO DE MAPA]")
        print(" 2) Mover empleado a otro grupo")
        print(" 3) Eliminar empleado del mapa")
        print(" 4) Eliminar grupo completo")
        print("")
        print("[ESTATUS PARA FALTAS]")
        print(" 5) Estatus para faltas: ACTIVO / BAJA")
        print("")
        print("[GUARDADO / SALIDA]")
        print(" 7) Guardar y salir")
        print(" 0) Salir sin guardar                        (volver al editor)")
        op = _safe_input("Opción: ", "").strip()

        if op == "0":
            return
        if op == "6":
            _print_groups(cfg)
            continue
        if op == "1":
            _bulk_assign_missing(processed_ids, cfg, script_dir, usuario)
            continue
        if op == "2":
            _move_employee(processed_ids, cfg, script_dir, usuario)
            continue
        if op == "3":
            _delete_employee(processed_ids, cfg, script_dir, usuario)
            continue
        if op == "4":
            _delete_group(cfg, script_dir, usuario)
            continue
        if op == "5":
            _toggle_status(processed_ids, cfg, script_dir, usuario)
            continue
        if op == "7":
            guardar_config(script_dir, cfg)
            print("Config guardada.")
            return
        print("Opción inválida.")
