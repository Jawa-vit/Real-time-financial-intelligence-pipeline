"""
common/indicators.py
---------------------
Pure, dependency-light functions for financial technical indicators.

Kept separate from Spark/Kafka code on purpose: these are plain Python
functions over lists/floats, which means they're trivially unit-testable
(see tests/test_pipeline.py) without spinning up a SparkSession.

The same logic is mirrored inside the PySpark jobs as Spark-native
window functions for performance at scale -- this module is the
"reference implementation" used to validate that the Spark version
produces correct results.
"""

from __future__ import annotations
from typing import List


def simple_moving_average(prices: List[float], window: int) -> float | None:
    """Average of the last `window` prices. Returns None if not enough data."""
    if window <= 0 or len(prices) < window:
        return None
    return sum(prices[-window:]) / window


def volatility(prices: List[float]) -> float | None:
    """Population standard deviation of prices, a simple volatility proxy."""
    n = len(prices)
    if n < 2:
        return None
    mean = sum(prices) / n
    variance = sum((p - mean) ** 2 for p in prices) / n
    return variance**0.5


def pct_change(current: float, previous: float) -> float | None:
    """Percentage change between two prices."""
    if previous in (0, None):
        return None
    return ((current - previous) / previous) * 100


def relative_strength_index(prices: List[float], period: int = 14) -> float | None:
    """
    Standard RSI calculation over the last `period` price changes.
    Returns a value between 0 and 100, or None if not enough data.
    """
    if len(prices) < period + 1:
        return None

    gains, losses = [], []
    window = prices[-(period + 1) :]
    for i in range(1, len(window)):
        delta = window[i] - window[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))
