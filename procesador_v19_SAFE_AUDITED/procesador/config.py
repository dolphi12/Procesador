"""Configuración y mapeos (grupos / IDGRUPO) - compatible con v18."""

from __future__ import annotations

import logging

import json
from .logger import log_exception
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from .utils import backup_file, chmod_restringido
from .validaciones import validate_non_negative_int, validate_weekday


@dataclass
class AppConfig:
    grupos_orden: List[str] = field(default_factory=lambda: ["000"])
    grupos_meta: Dict[str, Dict[str, str]] = field(default_factory=lambda: {"000": {"prefijo": "000"}})
    empleado_a_grupo: Dict[str, str] = field(default_factory=dict)
    empleado_a_idgrupo: Dict[str, str] = field(default_factory=dict)

    # Estado del empleado para cálculo de faltas / plantilla interna
    # - activo: True/False
    # - fecha_alta/fecha_baja: ISO (opcional)
    empleado_status: Dict[str, Dict[str, object]] = field(default_factory=dict)
    empleado_meta: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Prefijos/códigos de nómina globales (opcional).
    # Se pueden definir también por grupo en grupos_meta[GRUPO]['nomina_prefijos']
    nomina_prefijos: List[str] = field(default_factory=list)


    week_start_dow: int = 2
    dayfirst: bool = True
    umbral_extra_min: int = 480
    redondeo_extra_step_min: int = 1
    redondeo_extra_modo: str = "none"
    tope_descuento_comida_min: int = 30
    umbral_comida_media_hora_min: int = 60  # si comida <= 60min, se descuenta solo media hora (tope)
    id_min_width: int = 3
    idgrupo_emp_min_width: int = 2
    idgrupo_sep: str = "-"

    # ---- Auditoría ----
    app_name: str = "procesador"
    audit_dir_name: str = "auditoria"
    audit_index_filename: str = "auditoria_index.jsonl"
    audit_changes_filename: str = "auditoria_cambios.jsonl"
    audit_key_filename: str = "audit_key.txt"
    audit_key_storage: str = "appdata"  # appdata|script
    audit_key_dir: str = ""  # override absoluto/relativo (opcional)
    audit_signing_enabled: bool = True
    audit_rotate_max_bytes: int = 5_000_000

    # CLI / ejecución
    no_interactive_default: bool = False



    # Excel export formatting (determinístico / configurable)
    # - column_widths: ancho fijo por nombre de columna (exact match)
    # - column_width_patterns: lista de reglas regex para columnas dinámicas
    column_widths: Dict[str, float] = field(
        default_factory=lambda: {
            "Ajuste manual": 14,
            "Campo": 22,
            "Dias_presentes": 14,
            "Entrada": 14,
            "Fecha": 12,
            "Faltas": 14,
            "Horas extra": 18,
            "Horas trabajadas": 18,
            "ID": 12,
            "IDGRUPO": 12,
            "Mes": 10,
            "Miércoles": 14,
            "Nombre": 32,
            "Nota ajuste": 30,
            "Notas": 30,
            "Observaciones": 26,
            "Pases": 10,
            "Rango semana": 16,
            "Registro": 34,
            "Regreso de cenar": 14,
            "Regreso de comer": 14,
            "Salida": 14,
            "Salida a cenar": 14,
            "Salida a comer": 14,
            "Sábado": 14,
            "Semana": 10,
            "Total horas extra": 18,
            "Total horas trabajadas": 18,
            "Valor": 22,
            "Viernes": 14,
            "Domingo": 14,
            "Jueves": 14,
            "Lunes": 14,
            "Martes": 14,
        }
    )
    excel_idgrupo_split_by_group: bool = False

    column_width_patterns: List[Dict[str, object]] = field(
        default_factory=lambda: [
            {"pattern": r"^(Lun|Mar|Mié|Jue|Vie|Sáb|Dom)\s+\d{2}/\d{2}\s+", "width": 20},
        ]
    )

    def prefijo_de_grupo(self, grupo: str) -> str:
        return (self.grupos_meta.get(grupo, {}) or {}).get("prefijo", grupo)



def _config_path(script_dir: Path) -> Path:
    return script_dir / "mapa_grupos.json"


def cargar_config(script_dir: Path) -> AppConfig:
    path = _config_path(script_dir)
    if not path.exists():
        cfg = AppConfig()
        guardar_config(script_dir, cfg)
        return cfg
    data = json.loads(path.read_text(encoding="utf-8"))
    cfg = AppConfig()
    cfg.grupos_orden = data.get("grupos_orden", cfg.grupos_orden)
    cfg.grupos_meta = data.get("grupos_meta", cfg.grupos_meta)
    cfg.empleado_a_grupo = data.get("empleado_a_grupo", {})
    cfg.empleado_a_idgrupo = data.get("empleado_a_idgrupo", {})
    cfg.empleado_status = data.get("empleado_status", {}) or {}
    cfg.empleado_meta = data.get("empleado_meta", {}) or {}

    reglas = data.get("reglas", {}) or {}
    cfg.umbral_extra_min = int(reglas.get("umbral_extra_min", cfg.umbral_extra_min))
    cfg.redondeo_extra_step_min = int(reglas.get("redondeo_extra_step_min", cfg.redondeo_extra_step_min))
    cfg.redondeo_extra_modo = str(reglas.get("redondeo_extra_modo", cfg.redondeo_extra_modo))
    cfg.tope_descuento_comida_min = int(reglas.get("tope_descuento_comida_min", cfg.tope_descuento_comida_min))
    cfg.umbral_comida_media_hora_min = int(reglas.get("umbral_comida_media_hora_min", getattr(cfg, "umbral_comida_media_hora_min", 60)))
    cfg.id_min_width = int(reglas.get("id_min_width", cfg.id_min_width))
    cfg.week_start_dow = int(reglas.get("week_start_dow", getattr(cfg, "week_start_dow", 0)) or 0)
    cfg.dayfirst = bool(reglas.get("dayfirst", cfg.dayfirst))

    audit = data.get("audit", {}) or {}
    cfg.app_name = str(audit.get("app_name", cfg.app_name) or cfg.app_name)
    cfg.audit_dir_name = str(audit.get("audit_dir_name", cfg.audit_dir_name) or cfg.audit_dir_name)
    cfg.audit_index_filename = str(audit.get("audit_index_filename", cfg.audit_index_filename) or cfg.audit_index_filename)
    cfg.audit_changes_filename = str(audit.get("audit_changes_filename", cfg.audit_changes_filename) or cfg.audit_changes_filename)
    cfg.audit_key_filename = str(audit.get("audit_key_filename", cfg.audit_key_filename) or cfg.audit_key_filename)
    cfg.audit_key_storage = str(audit.get("audit_key_storage", cfg.audit_key_storage) or cfg.audit_key_storage)
    cfg.audit_key_dir = str(audit.get("audit_key_dir", cfg.audit_key_dir) or cfg.audit_key_dir)
    cfg.audit_signing_enabled = bool(audit.get("audit_signing_enabled", cfg.audit_signing_enabled))
    cfg.audit_rotate_max_bytes = int(audit.get("audit_rotate_max_bytes", cfg.audit_rotate_max_bytes) or cfg.audit_rotate_max_bytes)
    cfg.no_interactive_default = bool(audit.get("no_interactive_default", cfg.no_interactive_default))

    # Prefijos/códigos de nómina globales (lista de strings), opcional.
    try:
        cfg.nomina_prefijos = [str(x).strip() for x in (data.get('nomina_prefijos', []) or []) if str(x).strip()]
    except Exception:
        cfg.nomina_prefijos = []

    excel = data.get("excel", {}) or {}
    cfg.excel_idgrupo_split_by_group = bool(excel.get("idgrupo_split_by_group", getattr(cfg, "excel_idgrupo_split_by_group", True)))
    # Anchos exactos por nombre de columna
    cfg.column_widths = excel.get("column_widths", cfg.column_widths) or cfg.column_widths
    # Patrones (regex) para columnas dinámicas
    cfg.column_width_patterns = excel.get("column_width_patterns", cfg.column_width_patterns) or cfg.column_width_patterns

    # Validate numeric config bounds
    try:
        validate_non_negative_int(cfg.umbral_extra_min, "umbral_extra_min")
        validate_non_negative_int(cfg.redondeo_extra_step_min, "redondeo_extra_step_min")
        validate_non_negative_int(cfg.tope_descuento_comida_min, "tope_descuento_comida_min")
        validate_non_negative_int(cfg.umbral_comida_media_hora_min, "umbral_comida_media_hora_min")
        validate_non_negative_int(cfg.id_min_width, "id_min_width")
        validate_weekday(cfg.week_start_dow)
    except (TypeError, ValueError) as exc:
        log_exception(f"Valor inválido en config, usando defaults: {exc}", level=logging.WARNING)
        defaults = AppConfig()
        cfg.umbral_extra_min = max(0, cfg.umbral_extra_min)
        cfg.redondeo_extra_step_min = max(0, cfg.redondeo_extra_step_min)
        cfg.tope_descuento_comida_min = max(0, cfg.tope_descuento_comida_min)
        cfg.umbral_comida_media_hora_min = max(0, cfg.umbral_comida_media_hora_min)
        cfg.id_min_width = max(0, cfg.id_min_width)
        cfg.week_start_dow = cfg.week_start_dow % 7 if 0 <= cfg.week_start_dow else defaults.week_start_dow

    return cfg



def guardar_config(script_dir: Path, cfg: AppConfig) -> None:
    """Guarda configuración en mapa_grupos.json.

    Reglas:
    - No sobrescribe si el contenido serializado no cambió (evita backups/spam).
    - Si cambia y existe archivo previo, crea backup timestamped.
    - Endurece permisos best-effort (0600 en POSIX).

    Nota: este archivo es la fuente de verdad para:
      - grupos_orden / grupos_meta
      - empleado_a_grupo / empleado_a_idgrupo
      - empleado_status / empleado_meta (activos/bajas)
      - reglas de negocio (redondeos, tope, etc)
      - configuración de auditoría y export Excel
    """
    path = _config_path(script_dir)

    data = {
        "grupos_orden": cfg.grupos_orden,
        "grupos_meta": cfg.grupos_meta,
        "nomina_prefijos": getattr(cfg, "nomina_prefijos", []) or [],
        "empleado_a_grupo": cfg.empleado_a_grupo,
        "empleado_a_idgrupo": cfg.empleado_a_idgrupo,
        "empleado_status": cfg.empleado_status,
        "empleado_meta": cfg.empleado_meta,
        "reglas": {
            "umbral_extra_min": cfg.umbral_extra_min,
            "redondeo_extra_step_min": cfg.redondeo_extra_step_min,
            "redondeo_extra_modo": cfg.redondeo_extra_modo,
            "tope_descuento_comida_min": cfg.tope_descuento_comida_min,
            "umbral_comida_media_hora_min": getattr(cfg, "umbral_comida_media_hora_min", 60),
            "id_min_width": cfg.id_min_width,
            "week_start_dow": cfg.week_start_dow,
            "dayfirst": cfg.dayfirst,
        },
        "audit": {
            "app_name": cfg.app_name,
            "audit_dir_name": cfg.audit_dir_name,
            "audit_index_filename": cfg.audit_index_filename,
            "audit_changes_filename": cfg.audit_changes_filename,
            "audit_key_filename": cfg.audit_key_filename,
            "audit_key_storage": cfg.audit_key_storage,
            "audit_key_dir": cfg.audit_key_dir,
            "audit_signing_enabled": cfg.audit_signing_enabled,
            "audit_rotate_max_bytes": cfg.audit_rotate_max_bytes,
            "no_interactive_default": cfg.no_interactive_default,
        },
        "excel": {
            "idgrupo_split_by_group": bool(getattr(cfg, "excel_idgrupo_split_by_group", True)),
            "column_widths": cfg.column_widths,
            "column_width_patterns": cfg.column_width_patterns,
        },
    }

    new_text = json.dumps(data, ensure_ascii=False, indent=2)

    # Si no hay cambios, NO tocar el archivo (ni backups)
    try:
        if path.exists():
            old = path.read_text(encoding="utf-8")
            if old.strip() == new_text.strip():
                return
    except Exception:
        pass

    # Backup solo si existe archivo previo
    try:
        backup_file(path, suffix=".bak")
    except Exception:
        log_exception("Fallo best-effort en backup de config", level=logging.WARNING)

    path.write_text(new_text, encoding="utf-8")

    # Endurecer permisos del archivo
    try:
        chmod_restringido(path)
    except Exception:
        log_exception("Fallo best-effort en permisos de config", level=logging.WARNING)



load_config = cargar_config
save_config = guardar_config