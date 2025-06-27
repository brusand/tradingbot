from dataclasses import dataclass, field
from typing import Dict, Any, List, Type, Optional, Callable
import yaml
import pandas as pd
from datetime import datetime
import inspect
import functools


# Décorateur pour enregistrer les paramètres des indicateurs
def indicator_parameters(**defaults):
    def decorator(cls):
        cls._indicator_params = defaults
        return cls

    return decorator


# Classe de base pour tous les indicateurs
class Indicator:
    def __init__(self, parameters: Dict[str, Any]):
        self.params = self._validate_parameters(parameters)

    def _validate_parameters(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Valide les paramètres fournis par rapport aux paramètres attendus."""
        if not hasattr(self.__class__, '_indicator_params'):
            return parameters

        validated_params = {}
        for param, default in self.__class__._indicator_params.items():
            if param in parameters:
                validated_params[param] = parameters[param]
            else:
                validated_params[param] = default
        return validated_params

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Méthode à implémenter par chaque indicateur spécifique."""
        raise NotImplementedError("La méthode calculate doit être implémentée par la sous-classe")


# Registry pour gérer les indicateurs disponibles
class IndicatorRegistry:
    _instance = None
    _indicators: Dict[str, Type[Indicator]] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def register(cls, name: str):
        """Décorateur pour enregistrer une classe d'indicateur."""

        def decorator(indicator_class: Type[Indicator]):
            cls._indicators[name.upper()] = indicator_class
            return indicator_class

        return decorator

    @classmethod
    def get_indicator(cls, name: str, parameters: Dict[str, Any]) -> Indicator:
        """Crée une instance d'indicateur à partir de son nom et des paramètres."""
        indicator_class = cls._indicators.get(name.upper())
        if not indicator_class:
            raise ValueError(f"Indicateur non reconnu: {name}")
        return indicator_class(parameters)


# Implémentation des indicateurs spécifiques
@IndicatorRegistry.register('EMA')
@indicator_parameters(period=14, source='close', name=None)
class EMAIndicator(Indicator):
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.params['source']
        period = self.params['period']
        name = self.params['name'] or f"EMA_{period}"

        result = df[source].ewm(span=period, adjust=False).mean()
        return pd.DataFrame({name: result})


@IndicatorRegistry.register('MACD')
@indicator_parameters(fast=12, slow=26, signal=9, source='close', name=None)
class MACDIndicator(Indicator):
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        source = self.params['source']
        fast = self.params['fast']
        slow = self.params['slow']
        signal = self.params['signal']
        name = self.params['name'] or "MACD"

        ema_fast = df[source].ewm(span=fast, adjust=False).mean()
        ema_slow = df[source].ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        histogram = macd - signal_line

        return pd.DataFrame({
            f"{name}_FAST": ema_fast,
            f"{name}_SLOW": ema_slow,
            f"{name}": macd,
            f"{name}_SIGNAL": signal_line,
            f"{name}_HIST": histogram
        })