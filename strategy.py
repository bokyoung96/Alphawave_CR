import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Dict, Type


class AbstractStrategy(ABC):
    def __init__(self, period: int = 10):
        self.period = period

    @abstractmethod
    def generate_signal(self, prices: list) -> str:
        pass


class KaufmanAMAStrategy(AbstractStrategy):
    def __init__(self, period: int = 10, fast_period: int = 2, slow_period: int = 30):
        super().__init__(period)
        self.fast_period = fast_period
        self.slow_period = slow_period

    def calculate_ER(self, prices: list) -> float:
        change = abs(prices[-1] - prices[0])
        volatility = np.sum(np.abs(np.diff(prices)))
        return change / volatility if volatility != 0 else 0

    def calculate_SC(self, ER: float) -> float:
        fast_SC = 2 / (self.fast_period + 1)
        slow_SC = 2 / (self.slow_period + 1)
        return (ER * (fast_SC - slow_SC)) + slow_SC

    def calculate_AMA(self, prices: list) -> float:
        AMA = prices[0]
        for i in range(1, len(prices)):
            ER = self.calculate_ER(prices[:i+1])
            SC = self.calculate_SC(ER)
            AMA = AMA + SC * (prices[i] - AMA)
        return AMA

    def generate_signal(self, prices: list) -> str:
        if len(prices) < self.period:
            return 'hold'
        AMA_value = self.calculate_AMA(prices[-self.period:])
        current_price = prices[-1]

        if current_price > AMA_value:
            return 'buy'
        elif current_price < AMA_value:
            return 'sell'
        return 'hold'


class MovingAverageCrossStrategy(AbstractStrategy):
    def __init__(self, short_window: int = 5, long_window: int = 20):
        super().__init__(period=long_window)
        self.short_window = short_window
        self.long_window = long_window

    def generate_signal(self, prices: list) -> str:
        if len(prices) < self.long_window:
            return 'hold'
        short_ma = np.mean(prices[-self.short_window:])
        long_ma = np.mean(prices[-self.long_window:])
        if short_ma > long_ma:
            return 'buy'
        elif short_ma < long_ma:
            return 'sell'
        return 'hold'


@unique
class StrategyType(Enum):
    KAUFMAN_AMA = "KaufmanAMA"
    MA_CROSS = "MovingAverageCross"


strategy_classes: Dict[StrategyType, Type[AbstractStrategy]] = {
    StrategyType.KAUFMAN_AMA: KaufmanAMAStrategy,
    StrategyType.MA_CROSS: MovingAverageCrossStrategy,
}


def strategy_pool(strategy_type: StrategyType, **kwargs) -> AbstractStrategy:
    strategy_cls = strategy_classes.get(strategy_type)
    if strategy_cls:
        return strategy_cls(**kwargs)
    else:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
