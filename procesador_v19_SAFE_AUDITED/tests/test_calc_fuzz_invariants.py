
import random
from datetime import datetime, timedelta, time
from procesador.config import AppConfig
from procesador.core import normalize_registro_times, map_eventos, calcular_trabajado, minutos_entre

def _cfg():
    cfg = AppConfig()
    cfg.umbral_extra_min = 8 * 60
    cfg.tope_descuento_comida_min = 30
    cfg.umbral_comida_media_hora_min = 60
    cfg.redondeo_extra_step_min = 1
    cfg.redondeo_extra_modo = "none"
    return cfg

def _to_time(dt: datetime) -> time:
    return time(dt.hour, dt.minute)

def test_fuzz_invariants_500_cases():
    cfg = _cfg()
    rng = random.Random(12345)

    for _ in range(500):
        # elegir turno diurno o nocturno
        noct = rng.random() < 0.35
        if noct:
            ent = datetime(2000,1,1, rng.randint(20,23), rng.choice([0,5,10,15,20,25,30,35,40,45,50,55]))
            dur = rng.randint(6*60, 10*60)  # 6-10h
            sal = ent + timedelta(minutes=dur)
        else:
            ent = datetime(2000,1,1, rng.randint(6,10), rng.choice([0,5,10,15,20,25,30,35,40,45,50,55]))
            dur = rng.randint(6*60, 12*60)
            sal = ent + timedelta(minutes=dur)

        # generar 2..6 checadas (incluye breaks) dentro del turno
        n = rng.randint(2,6)
        points = sorted({0, dur} | {rng.randint(10, dur-10) for __ in range(n-2)})
        times_dt = [ent + timedelta(minutes=m) for m in points]
        times = [_to_time(t) for t in times_dt]

        times_norm, _ = normalize_registro_times(times)
        eventos = map_eventos(times_norm)

        # NoLaborado extra (0..2) dentro del turno
        no_lab=[]
        for k in range(rng.randint(0,2)):
            a = rng.randint(0, dur-5)
            b = a + rng.randint(1, min(60, dur-a))
            ini = _to_time(ent + timedelta(minutes=a))
            fin = _to_time(ent + timedelta(minutes=b))
            no_lab.append((ini, fin, f"nl{k}"))

        trabajado, extra, comida_ded, cena_ded, no_lab_ded, nolab_ov, nolab_intov, nolab_ign = calcular_trabajado(eventos, cfg, no_lab if no_lab else None)

        total = minutos_entre(eventos["Entrada"], eventos["Salida"])
        assert 0 <= trabajado <= total
        assert 0 <= extra <= trabajado
        assert 0 <= comida_ded <= total
        assert 0 <= cena_ded <= total
        assert 0 <= no_lab_ded <= total
        # Nunca descontar más que el total (con tolerancia de 0)
        assert comida_ded + cena_ded + no_lab_ded <= total
        # Solapes no duplican: el solape reportado no puede exceder el NoLaborado real
        assert 0 <= nolab_ov <= (sum(minutos_entre(a, b) for a, b, _ in no_lab) if no_lab else 0)
