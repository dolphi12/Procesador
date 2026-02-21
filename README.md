# Procesador

Procesador de asistencia para registros de HikCentral / iVMS. Calcula horas trabajadas, extras, descuentos de comida/cena, faltas y genera reportes en Excel.

## Requisitos

- Python 3.10+
- pandas ≥ 1.5
- openpyxl ≥ 3.0

## Instalación rápida

```bash
cd procesador_v19_SAFE_AUDITED
pip install -r requirements.txt
```

## Uso

```bash
python -m procesador.cli process --input ASISTENCIAS.xlsx --no-interactive
```

Para documentación completa (dashboard de revisión, modo interactivo, consolidación de archivos, auditoría), consulta [procesador_v19_SAFE_AUDITED/README.md](procesador_v19_SAFE_AUDITED/README.md).

## Tests

```bash
cd procesador_v19_SAFE_AUDITED
pip install pytest
python -m pytest -q
```
