"""Núcleo de cálculo (v18-compatible)."""

from __future__ import annotations

from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple

from .config import AppConfig

__all__ = [
    "add_minutes",
    "round_minutes",
    "minutos_entre",
    "normalize_registro_times",
    "map_eventos",
    "calcular_trabajado",
]

def add_minutes(t: Optional[time], minutes: int) -> Optional[time]:
    if t is None:
        return None
    dt = datetime(2000,1,1,t.hour,t.minute) + timedelta(minutes=minutes)
    return time(dt.hour, dt.minute)

def round_minutes(value_min: int, step: int, mode: str) -> int:
    """
    Redondea minutos a múltiplos de 'step'.
    mode:
      - 'up'      (hacia arriba)
      - 'down'    (hacia abajo)
      - 'nearest' (al más cercano)
    """
    if step <= 1:
        return max(0, value_min)
    v = max(0, value_min)
    if mode == "up":
        return ((v + step - 1) // step) * step
    if mode == "down":
        return (v // step) * step
    # nearest
    lo = (v // step) * step
    hi = lo + step
    return hi if (v - lo) >= (hi - v) else lo
def minutos_entre(t1: Optional[time], t2: Optional[time]) -> int:
    if t1 is None or t2 is None:
        return 0
    dt1 = datetime(2000, 1, 1, t1.hour, t1.minute)
    dt2 = datetime(2000, 1, 1, t2.hour, t2.minute)
    if dt2 < dt1:
        # cruce de medianoche (muy raro para checadas del mismo día, pero por seguridad)
        dt2 += timedelta(days=1)
    return int((dt2 - dt1).total_seconds() // 60)

def normalize_registro_times(times: List[time]) -> Tuple[List[time], bool]:
    """Normaliza y ordena checadas para cálculos consistentes.
    - Mantiene turnos nocturnos: si la entrada es tarde y hay horas menores, se asumen del día siguiente.
    - Reordena horas fuera de orden (por captura/pegado) para evitar duraciones 00:00 o negativas.
    Devuelve (lista_normalizada, reordenado_bool).
    """
    if not times or len(times) < 2:
        return times, False
    mins = [t.hour * 60 + t.minute for t in times]
    entry_min = mins[0]
    has_smaller = any(m < entry_min for m in mins[1:])
    span = max(mins) - min(mins)
    # Heurística de cruce de medianoche:
    # - Entrada tarde (>=18:00) y hay horas menores -> probable cruce
    # - O hay horas menores y el rango del día es muy amplio -> probable cruce
    wrap_likely = (entry_min >= 18 * 60 and has_smaller) or (has_smaller and span > 12 * 60)
    adjusted = []
    for idx, (t, m) in enumerate(zip(times, mins)):
        adj = m + (1440 if (wrap_likely and m < entry_min) else 0)
        adjusted.append((adj, idx, t))
    ordered = sorted(adjusted, key=lambda x: (x[0], x[1]))
    out = [t for _adj, _idx, t in ordered]

    return out, (out != times)
# ---------------------------
# Correcciones manuales (opcional)
# ---------------------------
CORR_EVENTOS = ["Entrada", "Salida a comer", "Regreso de comer", "Salida a cenar", "Regreso de cenar", "Salida"]

def map_eventos(times: List[time]) -> Dict[str, Optional[time]]:
    """
    Mapea checadas a eventos con la regla operativa solicitada:
      - 1 checada: Entrada

      - 2..N checadas: Primera = Entrada, Última = Salida
      - Intermedias (en orden):
          2ª = Salida a comer
          3ª = Regreso de comer
          4ª = Salida a cenar
          5ª = Regreso de cenar
    Si hay más de 6 checadas, se usan únicamente las 4 intermedias más tempranas
    y se conserva la última como Salida.
    """
    labels = ["Entrada", "Salida a comer", "Regreso de comer", "Salida a cenar", "Regreso de cenar", "Salida"]
    out = {k: None for k in labels}
    n = len(times)
    out["_extra_registros"] = max(0, n - 6)
    if n == 0:
        return out
    if n == 1:
        out["Entrada"] = times[0]
        return out
    # Siempre: primera = Entrada, última = Salida
    out["Entrada"] = times[0]
    out["Salida"] = times[-1]
    # Intermedias
    middle = times[1:-1]
    middle_labels = ["Salida a comer", "Regreso de comer", "Salida a cenar", "Regreso de cenar"]
    for lab, t in zip(middle_labels, middle[:4]):
        out[lab] = t
    return out

def calcular_trabajado(eventos: Dict[str, Optional[time]], cfg: AppConfig, no_laborado_extra: Optional[List[Tuple[Optional[time], Optional[time], str]]] = None) -> Tuple[int, int, int, int, int, int, int, int]:
    """
    Devuelve (minutos_trabajados, minutos_extra_redondeados).
    Enfoque (robusto y alineado a RRHH):
      1) Base = minutos totales entre Entrada y Salida (cruza medianoche si aplica).
      2) Restas por tiempo NO laborado:
         - Comida:
             * Si hay Salida a comer y Regreso de comer -> si duración <= umbral (60) descuenta min(duración, tope); si excede umbral descuenta duración completa
             * Si falta el regreso (pero hay salida final) -> asume fin = Salida y aplica la misma regla (umbral/tope)
         - Cena:
             * Si hay Salida a cenar y Regreso de cenar -> resta duración_real
             * Si falta el regreso (pero hay salida final) -> asume fin = Salida y resta duración_real
         - Salidas extraordinarias (por inconveniente):
             * Intervalos extra (inicio, fin). Si falta fin y hay salida final -> asume fin = Salida.
             * Se descuenta duración real (sin tope).
      3) Umbral de extra: a partir de cfg.umbral_extra_min (8:00) y redondeo según cfg.
    Nota: si falta Entrada o Salida, devuelve (0,0).
    """
    ent = eventos.get("Entrada")
    sal = eventos.get("Salida")
    if not ent or not sal:
        return 0, 0, 0, 0, 0, 0, 0, 0
    total = minutos_entre(ent, sal)
    # --- Comida (regla: si <= umbral, descuenta solo media hora; si excede, descuenta completo)
    sal_com = eventos.get("Salida a comer")
    reg_com = eventos.get("Regreso de comer")
    comida_ded = 0
    comida_fin_ventana = None  # fin de ventana descontada (para solapes con NoLaborado)
    if sal_com and (reg_com or sal):
        fin_real = reg_com if reg_com is not None else sal
        if fin_real:
            dur_real = minutos_entre(sal_com, fin_real)
            umbral = int(getattr(cfg, "umbral_comida_media_hora_min", 60))
            if dur_real <= umbral:
                comida_ded = int(min(dur_real, cfg.tope_descuento_comida_min))
                comida_fin_ventana = add_minutes(sal_com, comida_ded)
            else:
                comida_ded = int(dur_real)
                comida_fin_ventana = fin_real
    # --- Cena (real)
    sal_cen = eventos.get("Salida a cenar")
    reg_cen = eventos.get("Regreso de cenar")
    cena_ded = 0
    if sal_cen and reg_cen:
        cena_ded = minutos_entre(sal_cen, reg_cen)
    elif sal_cen and not reg_cen and sal:
        # caso incompleto: asume fin = Salida
        cena_ded = minutos_entre(sal_cen, sal)
    # --- Salidas extraordinarias (real) con validación de solapes
    extra_ded = 0
    nolab_overlap_cd = 0   # solape NoLaborado con comida/cena (no se duplica)
    nolab_solape_interno = 0  # solape entre intervalos NoLaborado (se fusionan)
    ignored_outside_shift = 0  # minutos de NoLaborado capturados fuera de la jornada
    # Construir ventanas "ya descontadas" para evitar doble descuento
    ventanas_descuento = []  # lista de (ini, fin)
    if sal_com and comida_fin_ventana:
        # Ventana de descuento de comida: hasta media hora (si <= umbral) o hasta regreso real (si excede umbral)
        ventanas_descuento.append((sal_com, comida_fin_ventana))
    if sal_cen:
        fin_cen = reg_cen if reg_cen is not None else sal
        if fin_cen:
            ventanas_descuento.append((sal_cen, fin_cen))
    # Helpers de línea de tiempo relativa a la jornada (soporta cruce de medianoche)
    crosses_midnight = sal < ent  # si la salida es "menor" que la entrada, cruza al día siguiente
    def _to_shift_dt(t: time) -> datetime:
        """Convierte una hora a datetime relativo a la jornada.
        En turnos nocturnos, horas menores que Entrada se consideran del día siguiente."""
        base = datetime(2000, 1, 1, t.hour, t.minute)
        if crosses_midnight and t < ent:
            base += timedelta(days=1)
        return base
    def _window(ent_t: time, sal_t: time) -> Tuple[datetime, datetime]:
        a = _to_shift_dt(ent_t)
        b = _to_shift_dt(sal_t)
        if b < a:
            b += timedelta(days=1)
        return a, b
    def _overlap_min(a1: time, a2: time, b1: time, b2: time) -> int:
        """Minutos de solape entre intervalos (a1,a2) y (b1,b2) en la misma línea temporal de jornada."""
        if not a1 or not a2 or not b1 or not b2:
            return 0
        A1, A2 = _to_shift_dt(a1), _to_shift_dt(a2)
        B1, B2 = _to_shift_dt(b1), _to_shift_dt(b2)
        if A2 < A1:
            A2 += timedelta(days=1)
        if B2 < B1:
            B2 += timedelta(days=1)
        s = max(A1, B1)

        e = min(A2, B2)
        if e <= s:
            return 0
        return int((e - s).total_seconds() // 60)
    def _clip_to_shift(ini_t: time, fin_t: time, ent_t: time, sal_t: time) -> Optional[Tuple[time, time, int]]:
        """Recorta un intervalo (ini, fin) a la ventana [Entrada, Salida] en la línea temporal de la jornada.
        Devuelve (ini_recortado, fin_recortado, minutos_ignorados_fuera_de_jornada)."""
        s0, s1 = _window(ent_t, sal_t)
        a0 = _to_shift_dt(ini_t)
        a1 = _to_shift_dt(fin_t)
        if a1 < a0:
            a1 += timedelta(days=1)
        i0 = max(a0, s0)
        i1 = min(a1, s1)
        if i1 <= i0:
            # todo fuera
            ignorados = int((a1 - a0).total_seconds() // 60)
            return None
        inside = int((i1 - i0).total_seconds() // 60)
        total_i = int((a1 - a0).total_seconds() // 60)
        ignorados = max(0, total_i - inside)
        return time(i0.hour, i0.minute), time(i1.hour, i1.minute), ignorados
    if no_laborado_extra:
        # Normalizar: convertir a lista de intervalos efectivos y ordenar por inicio
        intervals = []
        ignored_outside_shift = 0
        for ini, fin, _nota in no_laborado_extra:
            if ini is None:
                continue
            fin_eff = fin if fin is not None else sal
            if fin_eff is None:
                continue
            # Recortar a la jornada [Entrada, Salida] para evitar descuentos fuera de turno
            clipped = _clip_to_shift(ini, fin_eff, ent, sal)
            if clipped is None:
                # Intervalo completamente fuera de la jornada (se ignora pero se contabiliza como advertencia)
                try:
                    ignored_outside_shift += minutos_entre(ini, fin_eff)
                except Exception:
                    pass
                continue
            ini_c, fin_c, ign = clipped
            ignored_outside_shift += ign
            intervals.append((ini_c, fin_c))
        # Ordenar por inicio (considerando hora)
        # Ordenar por posición relativa desde Entrada (maneja turnos nocturnos)
        intervals.sort(key=lambda x: minutos_entre(ent, x[0]))
        # Fusionar solapes internos
        merged = []
        for ini, fin in intervals:
            if not merged:
                merged.append([ini, fin])
                continue
            last_ini, last_fin = merged[-1]
            # Si solapan o se enciman
            if _overlap_min(last_ini, last_fin, ini, fin) > 0 or (last_fin == ini):
                # calcular solape interno aproximado
                nolab_solape_interno += _overlap_min(last_ini, last_fin, ini, fin)
                # extender fin si es necesario
                # comparar fin en dt
                def _to_dt(t: time) -> datetime:
                    # Usar la misma línea temporal de la jornada para turnos nocturnos.
                    return _to_shift_dt(t)
                lf = _to_dt(last_fin); cf = _to_dt(fin)
                li = _to_dt(last_ini); ci = _to_dt(ini)
                if lf < li: lf += timedelta(days=1)
                if cf < ci: cf += timedelta(days=1)
                # mantener inicio más temprano (last_ini) y fin más tardío
                if cf > lf:
                    merged[-1][1] = fin
            else:

                merged.append([ini, fin])
        # Descontar merged evitando doble descuento con comida/cena
        for ini, fin in merged:
            dur = minutos_entre(ini, fin)
            ov = 0
            for v_ini, v_fin in ventanas_descuento:
                ov += _overlap_min(ini, fin, v_ini, v_fin)
            nolab_overlap_cd += ov
            extra_ded += max(0, dur - ov)
    trabajado = max(0, total - comida_ded - cena_ded - extra_ded)
    extra = max(0, trabajado - cfg.umbral_extra_min)
    # Horas extra SIN redondeo (se dejan exactas al minuto). Si en el futuro se desea,
    # se puede habilitar redondeo poniendo redondeo_extra_step_min > 1 y redondeo_extra_modo distinto de "none".
    if getattr(cfg, "redondeo_extra_step_min", 1) and cfg.redondeo_extra_step_min > 1 and getattr(cfg, "redondeo_extra_modo", "none") != "none":
        extra = round_minutes(extra, cfg.redondeo_extra_step_min, cfg.redondeo_extra_modo)
    return trabajado, extra, comida_ded, cena_ded, extra_ded, nolab_overlap_cd, nolab_solape_interno, ignored_outside_shift
# ---------------------------
# Lectura / procesamiento Excel
# ---------------------------
