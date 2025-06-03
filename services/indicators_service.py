import asyncio
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, UTC

from core.pubsub_engine import PubSubEngine, global_pubsub

"""
Système de registre d'indicateurs avec décorateurs pour l'auto-indexation
et la gestion automatique des paramètres par défaut.
"""

import functools
import inspect
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Callable, Optional
from dataclasses import dataclass, field


@dataclass
class IndicatorInfo:
    """Informations sur un indicateur"""
    name: str
    description: str
    default_params: Dict[str, Any]
    param_types: Dict[str, type]
    required_columns: List[str]
    output_columns: List[str]
    method: Callable
    category: str = "general"


class IndicatorRegistry:
    """Registre global des indicateurs"""

    _indicators: Dict[str, IndicatorInfo] = {}

    @classmethod
    def register(cls, indicator_info: IndicatorInfo):
        """Enregistre un indicateur dans le registre"""
        cls._indicators[indicator_info.name] = indicator_info

    @classmethod
    def get_indicator(cls, name: str) -> Optional[IndicatorInfo]:
        """Récupère un indicateur par son nom"""
        return cls._indicators.get(name)

    @classmethod
    def list_indicators(cls) -> Dict[str, IndicatorInfo]:
        """Liste tous les indicateurs disponibles"""
        return cls._indicators.copy()

    @classmethod
    def get_params(cls, name: str) -> Optional[Dict[str, Any]]:
        """Récupère les paramètres par défaut d'un indicateur"""
        indicator = cls.get_indicator(name)
        return indicator.default_params if indicator else None

    @classmethod
    def get_param_types(cls, name: str) -> Optional[Dict[str, type]]:
        """Récupère les types des paramètres d'un indicateur"""
        indicator = cls.get_indicator(name)
        return indicator.param_types if indicator else None

    @classmethod
    def call_indicator(cls, name: str, df: pd.DataFrame, **params) -> pd.Series:
        """Appelle un indicateur par son nom"""
        indicator = cls.get_indicator(name)
        if not indicator:
            raise ValueError(f"Indicateur '{name}' non trouvé")

        # Merger les paramètres par défaut avec ceux fournis
        final_params = {**indicator.default_params, **params}

        # Appeler la méthode
        return indicator.method(df, **final_params)


def indicator(name: str,
              description: str = "",
              default_params: Dict[str, Any] = None,
              required_columns: List[str] = None,
              output_columns: List[str] = None,
              category: str = "general"):
    """
    Décorateur pour enregistrer automatiquement un indicateur

    Args:
        name: Nom unique de l'indicateur
        description: Description de l'indicateur
        default_params: Paramètres par défaut
        required_columns: Colonnes requises dans le DataFrame
        output_columns: Colonnes de sortie (pour indicateurs multi-valeurs)
        category: Catégorie de l'indicateur
    """

    def decorator(func: Callable) -> Callable:
        # Analyser la signature de la fonction pour extraire les paramètres
        sig = inspect.signature(func)
        param_types = {}
        extracted_defaults = {}

        for param_name, param in sig.parameters.items():
            if param_name == 'df':  # Ignorer le DataFrame
                continue

            # Extraire le type
            param_types[param_name] = param.annotation if param.annotation != inspect.Parameter.empty else Any

            # Extraire la valeur par défaut
            if param.default != inspect.Parameter.empty:
                extracted_defaults[param_name] = param.default

        # Combiner avec les paramètres fournis au décorateur
        final_defaults = {**extracted_defaults, **(default_params or {})}

        # Créer les informations sur l'indicateur
        indicator_info = IndicatorInfo(
            name=name,
            description=description,
            default_params=final_defaults,
            param_types=param_types,
            required_columns=required_columns or ['close'],
            output_columns=output_columns or [name.lower()],
            method=func,
            category=category
        )

        # Enregistrer dans le registre
        IndicatorRegistry.register(indicator_info)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Ajouter des métadonnées à la fonction
        wrapper._indicator_info = indicator_info
        wrapper._indicator_name = name

        return wrapper

    return decorator


class TechnicalIndicators:
    """Classe contenant tous les indicateurs techniques avec décorateurs"""

    @staticmethod
    @indicator(
        name="SMA",
        description="Simple Moving Average",
        default_params={"period": 20, "source": "close"},
        required_columns=["close"],
        category="moving_averages"
    )
    async def simple_moving_average(df: pd.DataFrame, period: int = 20, source= "close", name: str = None) -> pd.DataFrame:
        """Calcule la moyenne mobile simple"""
        column_name = name or f"SMA_{period}"
        sma_values = df[source].rolling(window=period).mean()
        return pd.DataFrame({column_name: sma_values})


    @staticmethod
    @indicator(
        name="EMA",
        description="Exponential Moving Average",
        default_params={"period": 20, "source": "close"},
        required_columns=["close"],
        category="moving_averages"
    )
    async def exponential_moving_average(df: pd.DataFrame, period: int = 20, source="close", name: str = None) -> pd.DataFrame:
        """Calcule la moyenne mobile exponentielle"""
        column_name = name or f"EMA_{period}"
        ema_values = df[source].ewm(span=period).mean()
        return pd.DataFrame({column_name: ema_values})


    @staticmethod
    @indicator(
        name="RSI",
        description="Relative Strength Index",
        default_params={"period": 14},
        required_columns=["close"],
        category="oscillators"
    )
    async def relative_strength_index(df: pd.DataFrame, period: int = 14, name: str = None) -> pd.DataFrame:
        """Calcule le RSI"""
        column_name = name or f"RSI_{period}"

        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return pd.DataFrame({column_name: rsi})


    @staticmethod
    @indicator(
        name="MACD",
        description="Moving Average Convergence Divergence",
        default_params={"fast_period": 12, "slow_period": 26, "signal_period": 9, "source": "close"},
        required_columns=["close"],
        output_columns=["macd", "signal", "histogram"],
        category="momentum"
    )
    async def macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9,
             name: str = None) -> pd.DataFrame:
        """Calcule le MACD (retourne DataFrame avec 3 colonnes)"""
        base_name = name or "MACD"

        ema_fast = df['close'].ewm(span=fast_period).mean()
        ema_slow = df['close'].ewm(span=slow_period).mean()

        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period).mean()
        histogram = macd_line - signal_line

        return pd.DataFrame({
            f'{base_name}': macd_line,
            f'{base_name}_signal': signal_line,
            f'{base_name}_histogram': histogram
        })


    @staticmethod
    @indicator(
        name="BB",
        description="Bollinger Bands",
        default_params={"period": 20, "std_dev": 2},
        required_columns=["close"],
        output_columns=["bb_upper", "bb_middle", "bb_lower"],
        category="volatility"
    )
    async def bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2, name: str = None) -> pd.DataFrame:
        """Calcule les Bandes de Bollinger"""
        base_name = name or f"BB_{period}"

        sma = df['close'].rolling(window=period).mean()
        std = df['close'].rolling(window=period).std()

        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)

        return pd.DataFrame({
            f'{base_name}_upper': upper,
            f'{base_name}_middle': sma,
            f'{base_name}_lower': lower
        })


    @staticmethod
    @indicator(
        name="ATR",
        description="Average True Range",
        default_params={"period": 14},
        required_columns=["high", "low", "close"],
        category="volatility"
    )
    async def average_true_range(df: pd.DataFrame, period: int = 14, name: str = None) -> pd.DataFrame:
        """Calcule l'ATR"""
        column_name = name or f"ATR_{period}"

        high_low = df['high'] - df['low']
        high_close_prev = abs(df['high'] - df['close'].shift(1))
        low_close_prev = abs(df['low'] - df['close'].shift(1))

        true_range = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()

        return pd.DataFrame({column_name: atr})


    @staticmethod
    @indicator(
        name="STOCH",
        description="Stochastic Oscillator",
        default_params={"k_period": 14, "d_period": 3},
        required_columns=["high", "low", "close"],
        output_columns=["stoch_k", "stoch_d"],
        category="oscillators"
    )
    async def stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3, name: str = None) -> pd.DataFrame:
        """Calcule le Stochastique"""
        base_name = name or f"STOCH_{k_period}_{d_period}"

        lowest_low = df['low'].rolling(window=k_period).min()
        highest_high = df['high'].rolling(window=k_period).max()

        k_percent = 100 * ((df['close'] - lowest_low) / (highest_high - lowest_low))
        d_percent = k_percent.rolling(window=d_period).mean()

        return pd.DataFrame({
            f'{base_name}_K': k_percent,
            f'{base_name}_D': d_percent
        })

class IndicatorsService:
    """Service de calcul d'indicateurs techniques avec PubSub"""
    
    def __init__(self, pubsub: PubSubEngine = None):
        self.pubsub = pubsub or global_pubsub
        self.logger = logging.getLogger("IndicatorsService")
        
        # Cache pour optimiser les calculs
        self.indicators_cache: Dict[str, Dict] = {}

        self.registry = IndicatorRegistry

        # Statistiques
        self.stats = {
            "calculations_performed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0
        }

    async def start(self):
        """Démarre le service d'indicateurs"""
        await self.pubsub.start()
        
        # S'abonner aux demandes de calcul d'indicateurs
        await self.pubsub.subscribe(
            "indicators.calculate",
            self.on_calculate_request
        )
        
        self.logger.info("Indicators service started")
    
    async def stop(self):
        """Arrête le service d'indicateurs"""
        await self.pubsub.stop()
        self.logger.info("Indicators service stopped")

    async def list_available_indicators(self) -> pd.DataFrame:
        """Liste tous les indicateurs disponibles sous forme de DataFrame"""
        indicators = self.registry.list_indicators()

        data = []
        for name, info in indicators.items():
            # Formater les paramètres
            params_str = ", ".join([f"{k}={v}" for k, v in info.default_params.items()])

            data.append({
                'Name': name,
                'Description': info.description,
                'Category': info.category,
                'Default_Params': params_str,
                'Required_Columns': ", ".join(info.required_columns),
                'Output_Columns': ", ".join(info.output_columns)
            })

        return pd.DataFrame(data)

    async def get_indicator_params(self, name: str) -> Dict[str, Any]:
        """Récupère les paramètres d'un indicateur"""
        params = self.registry.get_params(name)
        if params is None:
            raise ValueError(f"Indicateur '{name}' non trouvé")
        return params

    async def get_indicator_param_types(self, name: str) -> Dict[str, type]:
        """Récupère les types des paramètres d'un indicateur"""
        param_types = self.registry.get_param_types(name)
        if param_types is None:
            raise ValueError(f"Indicateur '{name}' non trouvé")
        return param_types

    async def calculate(self, name: str, df: pd.DataFrame, **params) -> pd.Series:
        """Calcule un indicateur par son nom"""
        return self.registry.call_indicator(name, df, **params)

    async def on_calculate_request(self, request_data: Dict, metadata: Dict):
        """Traite une demande de calcul d'indicateur"""
        try:
            self.logger.info(f"Received calculation request: {request_data.keys()}")
            
            # Extraire les paramètres de la requête
            indicator_name = request_data.get("indicator")
            indicator_type = request_data.get("config", {}).get("type")
            parameters = request_data.get("config", {}).get("parameters", {})
            dataframe_data = request_data.get("dataframe", [])
            strategy_id = request_data.get("strategy_id")
            candle_timestamp = request_data.get("candle_timestamp")
            request_id = request_data.get("request_id")
            
            self.logger.info(f"Processing: {indicator_name} ({indicator_type}) for {strategy_id}")

            # Convertir en DataFrame
            df = pd.DataFrame(dataframe_data)
            if df.empty:
                self.logger.warning(f"Empty dataframe for indicator {indicator_name}")
                return
            
            self.logger.info(f"DataFrame created: {len(df)} rows, columns: {list(df.columns)}")
            
            # Vérifier le cache
            #cache_key = self._generate_cache_key(indicator_type, parameters, df)
            #cached_result = self._get_from_cache(cache_key)
            
            #if cached_result is not None:
            #    values = cached_result
            #    self.stats["cache_hits"] += 1
            #else:
                # Calculer l'indicateur
            #    values = await self._calculate_indicator(indicator_type, df, parameters)
            #    self._store_in_cache(cache_key, values)
            #    self.stats["cache_misses"] += 1
            #    self.stats["calculations_performed"] += 1
            self.logger.info(f"Calculating {indicator_type} with params: {parameters}")
            values = await self._calculate_indicator(indicator_type, df, parameters)
            self.logger.info(f"Calculation result: {len(values)} values, first few: {values[:5] if values else 'None'}")
            
            # Publier le résultat
            self.logger.info("About to publish result...")
            await self._publish_result(
                indicator_name,
                values,
                strategy_id,
                candle_timestamp,
                request_id,
                metadata.get("channel", "")
            )
            self.logger.info("Result published successfully")
            
        except Exception as e:
            self.logger.error(f"Error processing calculation request: {e}")
            import traceback
            traceback.print_exc()
            self.stats["errors"] += 1
    
    async def _calculate_indicator(self, indicator_type: str, df: pd.DataFrame, parameters: Dict) -> List[float]:
        """Calcule un indicateur spécifique"""
        # Utiliser les méthodes intégrées au lieu du registre
        if indicator_type == "SMA":
            return await self._calculate_sma(df, parameters)
        elif indicator_type == "EMA":
            return await self._calculate_ema(df, parameters)
        elif indicator_type == "RSI":
            return await self._calculate_rsi(df, parameters)
        elif indicator_type == "MACD":
            return await self._calculate_macd(df, parameters)
        elif indicator_type == "BB":
            return await self._calculate_bollinger(df, parameters)
        else:
            raise ValueError(f"Indicateur '{indicator_type}' non supporté")

    async def _calculate_sma(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Simple Moving Average"""
        period = params.get("period", 20)
        
        if len(df) < period:
            return [float('nan')] * len(df)
        
        sma = df['close'].rolling(window=period).mean()
        return sma.fillna(float('nan')).tolist()
    
    async def _calculate_ema(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Exponential Moving Average"""
        period = params.get("period", 20)
        
        if len(df) < period:
            return [float('nan')] * len(df)
        
        ema = df['close'].ewm(span=period, adjust=False).mean()
        return ema.fillna(float('nan')).tolist()
    
    async def _calculate_rsi(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Relative Strength Index"""
        period = params.get("period", 14)
        
        if len(df) < period + 1:
            return [float('nan')] * len(df)
        
        # Calcul RSI manuel
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.fillna(float('nan')).tolist()
    
    async def _calculate_macd(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule MACD"""
        fast_period = params.get("fast", 12)
        slow_period = params.get("slow", 26)
        signal_period = params.get("signal", 9)
        
        if len(df) < slow_period:
            return [float('nan')] * len(df)
        
        # Calculer EMA rapide et lente
        ema_fast = df['close'].ewm(span=fast_period).mean()
        ema_slow = df['close'].ewm(span=slow_period).mean()
        
        # MACD line
        macd_line = ema_fast - ema_slow
        
        # Signal line
        signal_line = macd_line.ewm(span=signal_period).mean()
        
        # Histogram
        histogram = macd_line - signal_line
        
        # Retourner la MACD line pour simplifier
        return macd_line.fillna(float('nan')).tolist()
    
    async def _calculate_bollinger(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Bollinger Bands (retourne la bande du milieu)"""
        period = params.get("period", 20)
        std_dev = params.get("std_dev", 2)
        
        if len(df) < period:
            return [float('nan')] * len(df)
        
        # Bande du milieu (SMA)
        middle_band = df['close'].rolling(window=period).mean()
        
        # Écart-type
        std = df['close'].rolling(window=period).std()
        
        # Bandes supérieure et inférieure
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        # Retourner la bande du milieu
        return middle_band.fillna(float('nan')).tolist()
    
    async def _calculate_stochastic(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Stochastic Oscillator"""
        k_period = params.get("k_period", 14)
        d_period = params.get("d_period", 3)
        
        if len(df) < k_period:
            return [float('nan')] * len(df)
        
        # %K
        lowest_low = df['low'].rolling(window=k_period).min()
        highest_high = df['high'].rolling(window=k_period).max()
        
        k_percent = 100 * ((df['close'] - lowest_low) / (highest_high - lowest_low))
        
        # %D (moyenne mobile de %K)
        d_percent = k_percent.rolling(window=d_period).mean()
        
        return k_percent.fillna(float('nan')).tolist()
    
    async def _calculate_atr(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Average True Range"""
        period = params.get("period", 14)
        
        if len(df) < 2:
            return [float('nan')] * len(df)
        
        # True Range
        high_low = df['high'] - df['low']
        high_close_prev = np.abs(df['high'] - df['close'].shift(1))
        low_close_prev = np.abs(df['low'] - df['close'].shift(1))
        
        true_range = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))
        
        # ATR (moyenne mobile du True Range)
        atr = pd.Series(true_range).rolling(window=period).mean()
        
        return atr.fillna(float('nan')).tolist()
    
    async def _publish_result(self, indicator_name: str, values: List[float], strategy_id: str, 
                             candle_timestamp: float, request_id: str, channel: str = ""):
        """Publie le résultat d'un calcul d'indicateur"""
        try:
            result_data = {
                "name": indicator_name,
                "values": values,
                "strategy_id": strategy_id,
                "candle_timestamp": candle_timestamp,
                "request_id": request_id,
                "timestamp": candle_timestamp
            }
            
            # Canal de résultat pour la stratégie
            result_channel = f"indicators.result.{strategy_id}"
            self.logger.info(f"Publishing to {result_channel} with data keys: {result_data.keys()}")
            
            # Debug: vérifier que PubSub est disponible
            if self.pubsub is None:
                self.logger.error("PubSub engine is None!")
                return
                
            await self.pubsub.publish(result_channel, result_data)
            
            self.logger.info(f"✅ Published {indicator_name} result to {result_channel} with {len(values)} values")
        except Exception as e:
            self.logger.error(f"❌ Error publishing result: {e}")
    
    async def _calculate_adx(self, df: pd.DataFrame, params: Dict) -> List[float]:
        """Calcule Average Directional Index"""
        period = params.get("period", 14)
        
        if len(df) < period + 1:
            return [float('nan')] * len(df)
        
        # Calcul simplifié d'ADX
        # Dans une implémentation complète, utilisez talib ou une librairie spécialisée
        
        # Pour maintenant, retourner une approximation basée sur ATR
        atr_values = await self._calculate_atr(df, {"period": period})
        
        # Normaliser pour simuler ADX (0-100)
        atr_series = pd.Series(atr_values)
        normalized = (atr_series / df['close']) * 100
        
        return normalized.fillna(float('nan')).tolist()
    
    def _generate_cache_key(self, indicator_type: str, parameters: Dict, df: pd.DataFrame) -> str:
        """Génère une clé de cache pour un calcul d'indicateur"""
        # Utiliser le hash des dernières valeurs pour détecter les changements
        if df.empty:
            return f"{indicator_type}_{parameters}_empty"
        
        last_rows = df.tail(5)  # Prendre les 5 dernières lignes
        data_hash = hash(str(last_rows.values.tobytes()))
        params_str = "_".join(f"{k}:{v}" for k, v in sorted(parameters.items()))
        
        return f"{indicator_type}_{params_str}_{len(df)}_{data_hash}"
    
    def _get_from_cache(self, cache_key: str) -> Optional[List[float]]:
        """Récupère un résultat du cache"""
        cache_data = self.indicators_cache.get(cache_key)
        if cache_data is None:
            return None
        
        # Vérifier l'âge du cache (max 1 minute)
        age = (datetime.now(UTC) - cache_data["timestamp"]).total_seconds()
        if age > 60:
            del self.indicators_cache[cache_key]
            return None
        
        return cache_data["values"]
    
    def _store_in_cache(self, cache_key: str, values: List[float]):
        """Stocke un résultat dans le cache"""
        # Limiter la taille du cache
        if len(self.indicators_cache) > 1000:
            # Supprimer les plus anciens
            oldest_key = min(self.indicators_cache.keys(), 
                           key=lambda k: self.indicators_cache[k]["timestamp"])
            del self.indicators_cache[oldest_key]
        
        self.indicators_cache[cache_key] = {
            "values": values,
            "timestamp": datetime.now(UTC)
        }
    
    async def _publish_result(self, indicator_name: str, values: List[float], 
                            strategy_id: str, candle_timestamp: float,
                            request_id: str, original_channel: str):
        """Publie le résultat du calcul"""
        # Extraire le canal de réponse du canal original
        if "calculate" in original_channel:
            response_channel = original_channel.replace("calculate", "result")
        else:
            response_channel = f"indicators.{strategy_id}.{indicator_name}.result"
        
        result_data = {
            "indicator": indicator_name,
            "values": values,
            "strategy_id": strategy_id,
            "candle_timestamp": candle_timestamp,
            "request_id": request_id,
            "calculated_at": datetime.now(UTC).isoformat(),
            "values_count": len(values)
        }
        
        await self.pubsub.publish(response_channel, result_data)
        
        self.logger.debug(f"Published result for {indicator_name} to {strategy_id}")
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques du service"""
        return {
            **self.stats,
            "cache_size": len(self.indicators_cache),
            "supported_indicators": list(self.indicator_functions.keys())
        }
    
    def clear_cache(self):
        """Vide le cache des indicateurs"""
        cache_size = len(self.indicators_cache)
        self.indicators_cache.clear()
        self.logger.info(f"Cleared indicators cache ({cache_size} entries)")


# Instance globale pour faciliter l'utilisation
global_indicators_service = IndicatorsService()