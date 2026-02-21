"""Tests for procesador.validaciones module."""

import pytest
from procesador.validaciones import (
    validate_input,
    validate_non_empty_string,
    validate_non_negative_int,
    validate_positive_integer,
    validate_range,
    validate_weekday,
)


class TestValidateInput:
    def test_valid_type(self):
        assert validate_input(42, int) == 42

    def test_valid_tuple_type(self):
        assert validate_input(3.14, (int, float)) == 3.14

    def test_invalid_type(self):
        with pytest.raises(TypeError, match="Expected value of type int"):
            validate_input("hello", int)


class TestValidateNonEmptyString:
    def test_valid(self):
        assert validate_non_empty_string("hello") == "hello"

    def test_strips_whitespace(self):
        assert validate_non_empty_string("  hello  ") == "hello"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            validate_non_empty_string("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError):
            validate_non_empty_string("   ")

    def test_non_string_raises(self):
        with pytest.raises(TypeError):
            validate_non_empty_string(123)


class TestValidatePositiveInteger:
    def test_valid(self):
        assert validate_positive_integer(5) == 5

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            validate_positive_integer(0)

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            validate_positive_integer(-1)


class TestValidateRange:
    def test_in_range(self):
        assert validate_range(5, 0, 10) == 5

    def test_at_bounds(self):
        assert validate_range(0, 0, 10) == 0
        assert validate_range(10, 0, 10) == 10

    def test_out_of_range(self):
        with pytest.raises(ValueError, match="must be between"):
            validate_range(11, 0, 10)


class TestValidateNonNegativeInt:
    def test_zero(self):
        assert validate_non_negative_int(0) == 0

    def test_positive(self):
        assert validate_non_negative_int(100) == 100

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="must be >= 0"):
            validate_non_negative_int(-1, "test_field")

    def test_non_int_raises(self):
        with pytest.raises(TypeError, match="expected int"):
            validate_non_negative_int("abc", "test_field")


class TestValidateWeekday:
    def test_valid_days(self):
        for d in range(7):
            assert validate_weekday(d) == d

    def test_too_high(self):
        with pytest.raises(ValueError, match="must be 0-6"):
            validate_weekday(7)

    def test_negative(self):
        with pytest.raises(ValueError, match="must be >= 0"):
            validate_weekday(-1)
