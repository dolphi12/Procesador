# Procesador de Asistencias HikCentral/iVMS

Herramienta modular para procesar registros de asistencia exportados desde **HikCentral** o **iVMS-4200**.
Calcula horas trabajadas, horas extra, descuento de comida/cena y faltas, y genera reportes en Excel.

## Requisitos

- Python 3.10+
- `pandas`, `openpyxl`

## Instalación rápida

```bash
cd procesador_v19_SAFE_AUDITED
pip install pandas openpyxl
```

## Ejecución

```bash
python -m procesador.cli process --input ASISTENCIAS.xlsx --no-interactive
```

Para más opciones y documentación completa, consulta
[procesador_v19_SAFE_AUDITED/README.md](procesador_v19_SAFE_AUDITED/README.md).

## Tests

```bash
cd procesador_v19_SAFE_AUDITED
pip install pytest pandas openpyxl
python -m pytest -q
```

