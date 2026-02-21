"""Funciones de validación de entrada para el procesador de asistencia."""

from __future__ import annotations


def validate_input(value: object, expected_type: type | tuple[type, ...]) -> object:
    """Validate input value against the expected type."""
    if not isinstance(value, expected_type):
        if isinstance(expected_type, tuple):
            names = ", ".join(t.__name__ for t in expected_type)
        else:
            names = expected_type.__name__
        raise TypeError(
            f"Expected value of type {names}, but got {type(value).__name__}."
        )
    return value


def validate_non_empty_string(value: object) -> str:
    """Validate that the string is not empty."""
    validate_input(value, str)
    s = str(value).strip()
    if not s:
        raise ValueError("String cannot be empty or just whitespace.")
    return s


def validate_positive_integer(value: object) -> int:
    """Validate that the value is a positive integer."""
    validate_input(value, int)
    v = int(value)  # type: ignore[arg-type]  # validated above
    if v <= 0:
        raise ValueError("Integer must be greater than zero.")
    return v


def validate_range(value: int | float, min_value: int | float, max_value: int | float) -> int | float:
    """Validate that the value is within the specified range."""
    validate_input(value, (int, float))
    if not (min_value <= value <= max_value):
        raise ValueError(f"Value {value} must be between {min_value} and {max_value}.")
    return value


def validate_non_negative_int(value: object, name: str = "value") -> int:
    """Validate that the value is a non-negative integer (>= 0)."""
    if not isinstance(value, int):
        raise TypeError(f"{name}: expected int, got {type(value).__name__}.")
    if value < 0:
        raise ValueError(f"{name}: must be >= 0, got {value}.")
    return value


def validate_weekday(value: int, name: str = "week_start_dow") -> int:
    """Validate weekday number (0=Monday .. 6=Sunday)."""
    validate_non_negative_int(value, name)
    if value > 6:
        raise ValueError(f"{name}: must be 0-6, got {value}.")
    return value
