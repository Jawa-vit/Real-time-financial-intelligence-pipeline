"""
tests/test_pipeline.py
------------------------
Unit tests for the dependency-light pieces of the pipeline: technical
indicator math and message schema validation. Deliberately does NOT spin up
Kafka, Spark, or Postgres -- those are exercised via the local docker-compose
stack, not CI unit tests.

Run:
    pytest tests/ -v
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import pytest
from jsonschema import validate, ValidationError

from common.indicators import (
    simple_moving_average,
    volatility,
    pct_change,
    relative_strength_index,
)
from common.schema import TICK_JSON_SCHEMA


class TestSimpleMovingAverage:
    def test_basic_average(self):
        assert simple_moving_average([10, 20, 30], 3) == 20

    def test_insufficient_data_returns_none(self):
        assert simple_moving_average([10, 20], 5) is None

    def test_window_of_zero_returns_none(self):
        assert simple_moving_average([10, 20, 30], 0) is None

    def test_uses_only_last_n_prices(self):
        assert simple_moving_average([100, 100, 10, 20, 30], 3) == 20


class TestVolatility:
    def test_zero_volatility_for_constant_prices(self):
        assert volatility([50, 50, 50, 50]) == 0

    def test_single_price_returns_none(self):
        assert volatility([50]) is None

    def test_volatility_increases_with_spread(self):
        low_spread = volatility([100, 101, 99, 100])
        high_spread = volatility([100, 150, 50, 100])
        assert high_spread > low_spread


class TestPctChange:
    def test_increase(self):
        assert pct_change(110, 100) == pytest.approx(10.0)

    def test_decrease(self):
        assert pct_change(90, 100) == pytest.approx(-10.0)

    def test_zero_previous_returns_none(self):
        assert pct_change(100, 0) is None


class TestRSI:
    def test_insufficient_data_returns_none(self):
        assert relative_strength_index([1, 2, 3], period=14) is None

    def test_all_gains_yields_rsi_100(self):
        prices = [float(i) for i in range(1, 17)]  # strictly increasing
        assert relative_strength_index(prices, period=14) == 100.0

    def test_rsi_within_bounds(self):
        prices = [100, 102, 101, 105, 103, 107, 110, 108, 112, 115, 113, 117, 120, 118, 122]
        rsi = relative_strength_index(prices, period=14)
        assert 0 <= rsi <= 100


class TestTickSchemaValidation:
    def valid_tick(self):
        return {
            "symbol": "AAPL",
            "price": 189.32,
            "open": 188.00,
            "day_high": 190.10,
            "day_low": 187.50,
            "prev_close": 188.50,
            "volume": 1234567,
            "currency": "USD",
            "event_time": "2026-06-19T14:30:00+00:00",
            "source": "yfinance",
        }

    def test_valid_tick_passes(self):
        validate(instance=self.valid_tick(), schema=TICK_JSON_SCHEMA)

    def test_missing_required_field_fails(self):
        tick = self.valid_tick()
        del tick["price"]
        with pytest.raises(ValidationError):
            validate(instance=tick, schema=TICK_JSON_SCHEMA)

    def test_wrong_type_fails(self):
        tick = self.valid_tick()
        tick["price"] = "not-a-number"
        with pytest.raises(ValidationError):
            validate(instance=tick, schema=TICK_JSON_SCHEMA)
