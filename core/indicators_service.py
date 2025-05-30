"""
Service de calcul d'indicateurs techniques
Calcule les indicateurs en parallèle pour les stratégies
"""

import asyncio
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from core.strategy_workflow import IndicatorConfig

logger = logging.getLogger(__name__)


class IndicatorsCalculator:
    """Calculateur d'indicateurs techniques"""
    
    @staticmethod
    def calculate_sma(data: pd.Series, period: int) -> pd.Series:
        """Simple Moving Average"""
        return data.rolling(window=period).mean()
    
    @staticmethod
    def calculate_ema(data: pd.Series, period: int) -> pd.Series:
        """Exponential Moving Average"""
        return data.ewm(span=period).mean()
    
    @staticmethod
    def calculate_rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """Relative Strength Index"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_macd(data: pd.Series, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, pd.Series]:
        """MACD (Moving Average Convergence Divergence)"""
        ema_fast = data.ewm(span=fast_period).mean()
        ema_slow = data.ewm(span=slow_period).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period).mean()
        histogram = macd_line - signal_line
        
        return {
            'MACD': macd_line,
            'Signal': signal_line,
            'Histogram': histogram
        }
    
    @staticmethod
    def calculate_bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2) -> Dict[str, pd.Series]:
        """Bollinger Bands"""
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        return {
            'BB_Upper': upper_band,
            'BB_Middle': sma,
            'BB_Lower': lower_band
        }
    
    @staticmethod
    def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average True Range"""
        high_low = high - low
        high_close = np.abs(high - close.shift())
        low_close = np.abs(low - close.shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        
        return atr
    
    @staticmethod
    def calculate_stochastic(high: pd.Series, low: pd.Series, close: pd.Series, 
                           k_period: int = 14, d_period: int = 3, smooth_k: int = 3) -> Dict[str, pd.Series]:
        """Stochastic Oscillator"""
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        
        k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
        k_percent_smooth = k_percent.rolling(window=smooth_k).mean()
        d_percent = k_percent_smooth.rolling(window=d_period).mean()
        
        return {
            'Stoch_K': k_percent_smooth,
            'Stoch_D': d_percent
        }


class IndicatorsService:
    """Service de calcul d'indicateurs avec PubSub"""
    
    def __init__(self, pubsub: PubSubEngine):
        self.pubsub = pubsub
        self.calculator = IndicatorsCalculator()
        self.is_running = False
        
        # Cache des calculs récents
        self.calculation_cache = {}
        self.cache_max_size = 1000
        
        # Métriques
        self.metrics = {
            'calculations_performed': 0,
            'cache_hits': 0,
            'errors': 0,
            'avg_calculation_time': 0.0
        }
    
    async def start_service(self):
        """Démarre le service d'indicateurs"""
        self.is_running = True
        
        # S'abonner aux demandes de calcul - utiliser un pattern plus large
        await self.pubsub.subscribe("indicators.calculate", self.on_calculation_request, weak_ref=False)
        
        logger.info("Indicators service started")
    
    async def stop_service(self):
        """Arrête le service d'indicateurs"""
        self.is_running = False
        logger.info("Indicators service stopped")
    
    async def on_calculation_request(self, request_data: Dict, metadata):
        """Traite une demande de calcul d'indicateur"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            logger.info(f"Received calculation request: {request_data.get('action')} for {request_data.get('indicator')}")
            
            # Extraire les données de la demande
            action = request_data.get("action")
            if action != "calculate":
                logger.debug(f"Ignoring action: {action}")
                return
            
            indicator_name = request_data.get("indicator")
            config = request_data.get("config", {})
            dataframe_data = request_data.get("dataframe", [])
            strategy_id = request_data.get("strategy_id")
            candle_timestamp = request_data.get("candle_timestamp")
            request_id = request_data.get("request_id")
            
            if not all([indicator_name, dataframe_data, strategy_id]):
                logger.warning("Missing required fields in calculation request")
                return
            
            # Vérifier le cache
            cache_key = self._generate_cache_key(request_id, indicator_name, config, len(dataframe_data))
            if cache_key in self.calculation_cache:
                logger.debug(f"Cache hit for {indicator_name}")
                result = self.calculation_cache[cache_key]
                self.metrics['cache_hits'] += 1
            else:
                # Calculer l'indicateur
                result = await self._calculate_indicator(indicator_name, config, dataframe_data)
                
                # Mettre en cache
                self._update_cache(cache_key, result)
                self.metrics['calculations_performed'] += 1
            
            # Publier le résultat
            await self._publish_result(
                indicator_name=indicator_name,
                values=result,
                strategy_id=strategy_id,
                candle_timestamp=candle_timestamp,
                request_id=request_id,
                config=config
            )
            
            # Mettre à jour les métriques
            calculation_time = asyncio.get_event_loop().time() - start_time
            self._update_metrics(calculation_time)
            
        except Exception as e:
            logger.error(f"Error in calculation request: {e}")
            self.metrics['errors'] += 1
    
    async def _calculate_indicator(self, indicator_name: str, config: Dict, dataframe_data: List[Dict]) -> List[float]:
        """Calcule un indicateur spécifique"""
        # Convertir en DataFrame
        df = pd.DataFrame(dataframe_data)
        
        if df.empty:
            return []
        
        # S'assurer que les colonnes sont numériques
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculer selon le type d'indicateur
        indicator_type = config.get('type', indicator_name).upper()
        parameters = config.get('parameters', {})
        source = config.get('source', 'close')
        
        try:
            if indicator_type == 'SMA':
                period = parameters.get('period', 20)
                result = self.calculator.calculate_sma(df[source], period)
                
            elif indicator_type == 'EMA':
                period = parameters.get('period', 20)
                result = self.calculator.calculate_ema(df[source], period)
                
            elif indicator_type == 'RSI':
                period = parameters.get('period', 14)
                result = self.calculator.calculate_rsi(df[source], period)
                
            elif indicator_type == 'MACD':
                fast_period = parameters.get('fast_period', 12)
                slow_period = parameters.get('slow_period', 26)
                signal_period = parameters.get('signal_period', 9)
                
                macd_data = self.calculator.calculate_macd(
                    df[source], fast_period, slow_period, signal_period
                )
                # Pour MACD, on retourne la ligne MACD par défaut
                result = macd_data['MACD']
                
            elif indicator_type in ['BB', 'BOLLINGERBANDS']:
                period = parameters.get('period', 20)
                std_dev = parameters.get('std_dev', 2)
                
                bb_data = self.calculator.calculate_bollinger_bands(
                    df[source], period, std_dev
                )
                # Pour BB, on retourne la bande du milieu par défaut
                result = bb_data['BB_Middle']
                
            elif indicator_type == 'ATR':
                period = parameters.get('period', 14)
                result = self.calculator.calculate_atr(
                    df['high'], df['low'], df['close'], period
                )
                
            elif indicator_type in ['STOCH', 'STOCHASTIC']:
                k_period = parameters.get('k_period', 14)
                d_period = parameters.get('d_period', 3)
                smooth_k = parameters.get('smooth_k', 3)
                
                stoch_data = self.calculator.calculate_stochastic(
                    df['high'], df['low'], df['close'], k_period, d_period, smooth_k
                )
                # Pour Stoch, on retourne %K par défaut
                result = stoch_data['Stoch_K']
                
            else:
                logger.warning(f"Unknown indicator type: {indicator_type}")
                return []
            
            # Convertir en liste en gérant les NaN
            values = result.fillna(0).tolist()
            logger.debug(f"Calculated {indicator_type} with {len(values)} values")
            
            return values
            
        except Exception as e:
            logger.error(f"Error calculating {indicator_type}: {e}")
            return []
    
    async def _publish_result(self, indicator_name: str, values: List[float], 
                            strategy_id: str, candle_timestamp: float, 
                            request_id: str, config: Dict):
        """Publie le résultat du calcul"""
        try:
            # Publier vers le channel spécifique de la stratégie
            channel = f"indicators.result.{strategy_id}"
            
            result_data = {
                "indicator": indicator_name,
                "values": values,
                "strategy_id": strategy_id,
                "candle_timestamp": candle_timestamp,
                "request_id": request_id,
                "config": config,
                "calculated_at": datetime.now(timezone.utc).timestamp(),
                "values_count": len(values)
            }
            
            await self.pubsub.publish(channel, result_data)
            logger.info(f"Published {indicator_name} result to {channel} with {len(values)} values")
            
        except Exception as e:
            logger.error(f"Error publishing result for {indicator_name}: {e}")
    
    def _generate_cache_key(self, request_id: str, indicator_name: str, 
                          config: Dict, data_length: int) -> str:
        """Génère une clé de cache pour un calcul"""
        # Simplification : utiliser les paramètres principaux
        params_str = str(sorted(config.get('parameters', {}).items()))
        return f"{indicator_name}_{params_str}_{data_length}"
    
    def _update_cache(self, cache_key: str, result: List[float]):
        """Met à jour le cache avec un nouveau résultat"""
        if len(self.calculation_cache) >= self.cache_max_size:
            # Supprimer le plus ancien (simple FIFO)
            oldest_key = next(iter(self.calculation_cache))
            del self.calculation_cache[oldest_key]
        
        self.calculation_cache[cache_key] = result
    
    def _update_metrics(self, calculation_time: float):
        """Met à jour les métriques de performance"""
        if self.metrics['avg_calculation_time'] == 0:
            self.metrics['avg_calculation_time'] = calculation_time
        else:
            # Moyenne mobile
            self.metrics['avg_calculation_time'] = (
                self.metrics['avg_calculation_time'] * 0.9 + 
                calculation_time * 0.1
            )
    
    def get_metrics(self) -> Dict:
        """Retourne les métriques du service"""
        return {
            **self.metrics,
            'cache_size': len(self.calculation_cache),
            'cache_hit_rate': (
                self.metrics['cache_hits'] / 
                max(1, self.metrics['cache_hits'] + self.metrics['calculations_performed'])
            ) * 100
        }