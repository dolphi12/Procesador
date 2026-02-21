# Procesador v19 (modular - migración)

Este paquete envuelve el comportamiento de `PROCESADOR_v18_ESTABLE` (incluido como `procesador/legacy.py`) y agrega:

- CLI modular (`procesador/cli.py`)
- Edición interactiva (opcional) con auditoría (JSONL + bundle firmado)
- Revisión/edición **por ID** post-proceso (dashboard textual en cascada) + edición de NoLaborado
- Backups automáticos antes de sobrescribir salidas

## Ejecutar (modo automático)

```bash
python -m procesador.cli --input "ASISTENCIAS.xlsx"
```

## Dashboard de revisión por ID (cascada)

Para editar cualquier empleado por su ID (aunque no tenga anomalías), sin que el programa pregunte 1x1:

```bash
python -m procesador.cli --input "ASISTENCIAS.xlsx" --review --usuario "RRHH"
```

Dentro del dashboard puedes editar:
- checadas (editar/insertar/borrar)
- permiso **NoLaborado** (intervalos) desde la opción **8**

Al entrar al dashboard se muestra un **resumen de incidencias** (conteo por tipo) y desde el menú puedes:
- listar incidencias (top 30) y abrir el editor directo
- filtrar incidencias por tipo

> Nota: en esta versión ya no se carga una "lista manual" dentro del script; las correcciones se realizan desde el dashboard y quedan auditadas.

## Modo interactivo (por registro)

> Nota: esto pregunta por cada registro (ID/Fecha) si deseas editar. Úsalo solo cuando lo necesites.

```bash
python -m procesador.cli --input "ASISTENCIAS.xlsx" --interactive --usuario "RRHH"
```

Solo anomalias:

```bash
python -m procesador.cli --input "ASISTENCIAS.xlsx" --interactive --interactive-anomalias
```

## Salidas

Genera:
- `*_PROCESADO.xlsx` con hojas: `Reporte`, `RESUMEN_SEMANAL`, `RESUMEN_MENSUAL`, `DETALLE_FALTAS`, `INCIDENCIAS`, `RESUM_SEM_CHECADAS`
- `*_IDGRUPO.xlsx` con las mismas hojas pero usando IDGRUPO en lugar de ID (donde aplique)
- `auditoria/` con trazabilidad completa (si hubo ediciones o se ejecutó en modo verify)

## Tests

```bash
pip install pytest
pytest
```

## Flags útiles

- `--dry-run`: simula; no escribe Excel, no modifica config, no genera auditoría.
- `--no-interactive`: modo batch; desactiva dashboards/prompts.
- `--tests-only`: ejecuta `pytest -q` y termina.
- `verify-audit`: verifica firma del bundle.

Ejemplo:

```bash
python -m procesador.cli process --input ASISTENCIAS.xlsx --verify --no-interactive
```


## Consolidar archivos diarios (cierre semanal/mensual)

En operación diaria normalmente procesas 1 archivo por día. Para cierres semanales/mensuales, puedes consolidar una carpeta de archivos diarios en un solo Excel y procesarlo.

### 1) Solo consolidar (merge)

```bash
python -m procesador.cli merge --input-dir ./inputs_semana --pattern "*.xlsx" --output ./consolidado_semana.xlsx
```

### 2) Consolidar y procesar en una sola corrida

```bash
python -m procesador.cli process --input-dir ./inputs_semana --pattern "*.xlsx" --merge-output ./consolidado_semana.xlsx --plantilla ./plantilla_empleados.xlsx
```

Notas:
- El merge elimina duplicados exactos por (ID, Nombre, Fecha, Registro).
- El merge ordena por Fecha y luego por ID/Nombre.
