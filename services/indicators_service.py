import asyncio
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, UTC
import talib

from core.pubsub_engine import PubSubEngine, global_pubsub


class IndicatorsService:
    """Service de calcul d'indicateurs techniques avec PubSub"""
    
    def __init__(self, pubsub: PubSubEngine = None):
        self.pubsub = pubsub or global_pubsub
        self.logger = logging.getLogger("IndicatorsService")
        
        # Cache pour optimiser les calculs
        self.indicators_cache: Dict[str, Dict] = {}
        
        # Statistiques
        self.stats = {
            "calculations_performed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0
        }
        
        # Mapping des types d'indicateurs vers les fonctions
        self.indicator_functions = {
            "SMA": self._calculate_sma,
            "EMA": self._calculate_ema,
            "RSI": self._calculate_rsi,
            "MACD": self._calculate_macd,
            "BOLLINGER": self._calculate_bollinger,
            "STOCH": self._calculate_stochastic,
            "ATR": self._calculate_atr,
            "ADX": self._calculate_adx
        }
    
    async def start(self):
        """Démarre le service d'indicateurs"""
        await self.pubsub.start()
        
        # S'abonner aux demandes de calcul d'indicateurs
        await self.pubsub.subscribe(
            "indicators.*.*.calculate",
            self.on_calculate_request
        )
        
        self.logger.info("Indicators service started")
    
    async def stop(self):
        """Arrête le service d'indicateurs"""
        await self.pubsub.stop()
        self.logger.info("Indicators service stopped")
    
    async def on_calculate_request(self, request_data: Dict, metadata: Dict):
        """Traite une demande de calcul d'indicateur"""
        try:
            # Extraire les paramètres de la requête
            indicator_name = request_data.get("indicator")
            indicator_type = request_data.get("config", {}).get("type")
            parameters = request_data.get("config", {}).get("parameters", {})
            dataframe_data = request_data.get("dataframe", [])
            strategy_id = request_data.get("strategy_id")
            candle_timestamp = request_data.get("candle_timestamp")
            request_id = request_data.get("request_id")
            
            # Validation
            if not all([indicator_name, indicator_type, dataframe_data]):
                self.logger.error(f"Invalid calculation request: missing required fields")
                return
            
            # Convertir en DataFrame
            df = pd.DataFrame(dataframe_data)
            if df.empty:
                self.logger.warning(f"Empty dataframe for indicator {indicator_name}")
                return
            
            # Vérifier le cache
            cache_key = self._generate_cache_key(indicator_type, parameters, df)
            cached_result = self._get_from_cache(cache_key)
            
            if cached_result is not None:
                values = cached_result
                self.stats["cache_hits"] += 1
            else:
                # Calculer l'indicateur
                values = await self._calculate_indicator(indicator_type, df, parameters)
                self._store_in_cache(cache_key, values)
                self.stats["cache_misses"] += 1
                self.stats["calculations_performed"] += 1
            
            # Publier le résultat
            await self._publish_result(
                indicator_name,
                values,
                strategy_id,
                candle_timestamp,
                request_id,
                metadata.get("channel", "")
            )
            
        except Exception as e:
            self.logger.error(f"Error processing calculation request: {e}")
            self.stats["errors"] += 1
    
    async def _calculate_indicator(self, indicator_type: str, df: pd.DataFrame, parameters: Dict) -> List[float]:
        """Calcule un indicateur spécifique"""
        if indicator_type not in self.indicator_functions:
            raise ValueError(f"Unknown indicator type: {indicator_type}")
        
        calculator = self.indicator_functions[indicator_type]
        return await calculator(df, parameters)
    
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