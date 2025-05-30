"""
Strategy Manager Enhanced avec Queue Workflow
Intègre le nouveau workflow de stratégie avec le système existant
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from core.strategy_workflow import TradingStrategy, IndicatorConfig, SignalRule
from core.indicators_service import IndicatorsService
from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from data.models import StrategyConfig, SessionMode
from data.persistence import DatabaseManager
from strategies.performance_tracker import PerformanceTracker

logger = logging.getLogger(__name__)


class EnhancedStrategyConfig:
    """Configuration de stratégie enrichie pour le workflow"""
    
    def __init__(self, base_config: StrategyConfig):
        # Copier les propriétés de base
        self.name = base_config.name
        self.pairs = base_config.pairs
        self.timeframe = base_config.timeframe
        self.parameters = base_config.parameters
        
        # Nouvelles propriétés pour le workflow
        self.max_dataframe_size = 1000
        self.indicators = {}
        self.signal_rules = []
        
        # Convertir les paramètres en configuration d'indicateurs
        self._setup_indicators_from_parameters()
        self._setup_default_signal_rules()
    
    def _setup_indicators_from_parameters(self):
        """Configure les indicateurs basés sur les paramètres"""
        # Vérifier s'il y a des paramètres short_window et long_window
        if 'short_window' in self.parameters and 'long_window' in self.parameters:
            short_window = self.parameters.get('short_window', 10)
            long_window = self.parameters.get('long_window', 30)
            
            self.indicators = {
                f"SMA_{short_window}": IndicatorConfig(
                    type="SMA",
                    parameters={"period": short_window},
                    source="close"
                ),
                f"SMA_{long_window}": IndicatorConfig(
                    type="SMA", 
                    parameters={"period": long_window},
                    source="close"
                ),
                "RSI_14": IndicatorConfig(
                    type="RSI",
                    parameters={"period": 14},
                    source="close"
                )
            }
        # Indicateurs par défaut basés sur le nom de la stratégie
        elif "sma" in self.name.lower() or "moving" in self.name.lower():
            short_window = self.parameters.get('short_window', 10)
            long_window = self.parameters.get('long_window', 30)
            
            self.indicators = {
                f"SMA_{short_window}": IndicatorConfig(
                    type="SMA",
                    parameters={"period": short_window},
                    source="close"
                ),
                f"SMA_{long_window}": IndicatorConfig(
                    type="SMA", 
                    parameters={"period": long_window},
                    source="close"
                ),
                "RSI_14": IndicatorConfig(
                    type="RSI",
                    parameters={"period": 14},
                    source="close"
                )
            }
        
        elif "ema" in self.name.lower():
            self.indicators = {
                "EMA_12": IndicatorConfig(
                    type="EMA",
                    parameters={"period": 12},
                    source="close"
                ),
                "EMA_26": IndicatorConfig(
                    type="EMA",
                    parameters={"period": 26},
                    source="close"
                ),
                "MACD": IndicatorConfig(
                    type="MACD",
                    parameters={"fast_period": 12, "slow_period": 26, "signal_period": 9},
                    source="close"
                )
            }
        
        else:
            # Configuration par défaut
            self.indicators = {
                "SMA_20": IndicatorConfig(
                    type="SMA",
                    parameters={"period": 20},
                    source="close"
                ),
                "EMA_50": IndicatorConfig(
                    type="EMA",
                    parameters={"period": 50},
                    source="close"
                ),
                "RSI_14": IndicatorConfig(
                    type="RSI",
                    parameters={"period": 14},
                    source="close"
                )
            }
    
    def _setup_default_signal_rules(self):
        """Configure les règles de signal par défaut"""
        # Vérifier s'il y a des paramètres short_window et long_window
        if 'short_window' in self.parameters and 'long_window' in self.parameters:
            short_window = self.parameters.get('short_window', 10)
            long_window = self.parameters.get('long_window', 30)
            
            self.signal_rules = [
                SignalRule(
                    name="sma_crossover_long",
                    signal_type="LONG",
                    condition=f"SMA_{short_window} > SMA_{long_window} & SMA_{short_window}[-1] <= SMA_{long_window}[-1] & RSI_14 < 70",
                    priority=2
                ),
                SignalRule(
                    name="sma_crossover_short",
                    signal_type="SHORT", 
                    condition=f"SMA_{short_window} < SMA_{long_window} & SMA_{short_window}[-1] >= SMA_{long_window}[-1] & RSI_14 > 30",
                    priority=2
                )
            ]
        elif "sma" in self.name.lower():
            short_window = self.parameters.get('short_window', 10)
            long_window = self.parameters.get('long_window', 30)
            
            self.signal_rules = [
                SignalRule(
                    name="sma_crossover_long",
                    signal_type="LONG",
                    condition=f"SMA_{short_window} > SMA_{long_window} & SMA_{short_window}[-1] <= SMA_{long_window}[-1] & RSI_14 < 70",
                    priority=2
                ),
                SignalRule(
                    name="sma_crossover_short",
                    signal_type="SHORT", 
                    condition=f"SMA_{short_window} < SMA_{long_window} & SMA_{short_window}[-1] >= SMA_{long_window}[-1] & RSI_14 > 30",
                    priority=2
                )
            ]
        
        elif "ema" in self.name.lower():
            self.signal_rules = [
                SignalRule(
                    name="ema_trend_long",
                    signal_type="LONG",
                    condition="EMA_12 > EMA_26 & close > EMA_12 & EMA_12 > EMA_12[-1]",
                    priority=1
                ),
                SignalRule(
                    name="ema_trend_short",
                    signal_type="SHORT",
                    condition="EMA_12 < EMA_26 & close < EMA_12 & EMA_12 < EMA_12[-1]",
                    priority=1
                )
            ]
        
        else:
            # Règles par défaut
            self.signal_rules = [
                SignalRule(
                    name="trend_following_long",
                    signal_type="LONG",
                    condition="close > SMA_20 & EMA_50 > EMA_50[-1] & RSI_14 > 50 & RSI_14 < 70",
                    priority=1
                ),
                SignalRule(
                    name="trend_following_short",
                    signal_type="SHORT",
                    condition="close < SMA_20 & EMA_50 < EMA_50[-1] & RSI_14 < 50 & RSI_14 > 30",
                    priority=1
                )
            ]


class StrategyManagerEnhanced:
    """Manager de stratégies avec workflow avancé"""
    
    def __init__(self, db_manager: DatabaseManager, pubsub: PubSubEngine):
        self.db_manager = db_manager
        self.pubsub = pubsub
        
        # Services
        self.indicators_service = IndicatorsService(pubsub)
        
        # Stratégies actives
        self.active_strategies: Dict[str, TradingStrategy] = {}
        
        # Métriques globales
        self.global_metrics = {
            'total_strategies': 0,
            'active_strategies': 0,
            'total_signals_generated': 0,
            'total_candles_processed': 0,
            'service_start_time': None
        }
    
    async def initialize(self):
        """Initialise le manager de stratégies"""
        # Démarrer le service d'indicateurs
        await self.indicators_service.start_service()
        
        self.global_metrics['service_start_time'] = datetime.now(timezone.utc)
        logger.info("Enhanced Strategy Manager initialized")
    
    async def start_strategy(self, session_id: str, strategy_config: StrategyConfig, 
                           api_key: str = "", api_secret: str = "") -> bool:
        """Démarre une stratégie avec le nouveau workflow"""
        try:
            if session_id in self.active_strategies:
                logger.warning(f"Strategy {session_id} already running")
                return False
            
            # Créer une configuration enrichie
            enhanced_config = EnhancedStrategyConfig(strategy_config)
            
            # Créer et démarrer la stratégie
            strategy = TradingStrategy(enhanced_config, self.pubsub)
            await strategy.start_strategy()
            
            # Enregistrer la stratégie
            self.active_strategies[session_id] = strategy
            
            # S'abonner aux signaux pour le tracking
            await self._subscribe_to_strategy_signals(session_id, strategy)
            
            # Mettre à jour les métriques
            self.global_metrics['total_strategies'] += 1
            self.global_metrics['active_strategies'] = len(self.active_strategies)
            
            logger.info(f"Strategy {session_id} started with enhanced workflow")
            return True
            
        except Exception as e:
            logger.error(f"Error starting strategy {session_id}: {e}")
            return False
    
    async def stop_strategy(self, session_id: str) -> bool:
        """Arrête une stratégie"""
        try:
            if session_id not in self.active_strategies:
                logger.warning(f"Strategy {session_id} not found")
                return False
            
            strategy = self.active_strategies[session_id]
            await strategy.stop_strategy()
            
            # Retirer de la liste active
            del self.active_strategies[session_id]
            
            # Mettre à jour les métriques
            self.global_metrics['active_strategies'] = len(self.active_strategies)
            
            logger.info(f"Strategy {session_id} stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping strategy {session_id}: {e}")
            return False
    
    async def pause_strategy(self, session_id: str) -> bool:
        """Met en pause une stratégie"""
        try:
            if session_id not in self.active_strategies:
                logger.warning(f"Strategy {session_id} not found")
                return False
            
            strategy = self.active_strategies[session_id]
            await strategy.stop_strategy()  # Pour l'instant, pause = stop
            
            logger.info(f"Strategy {session_id} paused")
            return True
            
        except Exception as e:
            logger.error(f"Error pausing strategy {session_id}: {e}")
            return False
    
    async def get_strategy_status(self, session_id: str) -> Optional[Dict]:
        """Récupère le status d'une stratégie"""
        if session_id not in self.active_strategies:
            return None
        
        strategy = self.active_strategies[session_id]
        
        return {
            'session_id': session_id,
            'name': strategy.config.name,
            'is_running': strategy.is_running,
            'processing_candle': strategy.processing_candle,
            'queue_size': strategy.candle_queue.qsize(),
            'dataframe_info': strategy.get_dataframe_info(),
            'metrics': strategy.processing_metrics,
            'last_update': datetime.now(timezone.utc).isoformat()
        }
    
    async def get_strategy_performance(self, session_id: str) -> Optional[Dict]:
        """Récupère les performances d'une stratégie"""
        if session_id not in self.active_strategies:
            return None
        
        strategy = self.active_strategies[session_id]
        
        # Construire les métriques de performance depuis le DataFrame
        df_info = strategy.get_dataframe_info()
        
        if not df_info.get('last_values'):
            return None
        
        last_values = df_info['last_values']
        
        return {
            'current_capital': last_values.get('balance', 10000),
            'total_pnl': last_values.get('balance', 10000) - 10000,  # Simplifié
            'total_pnl_pct': ((last_values.get('balance', 10000) - 10000) / 10000) * 100,
            'total_trades': strategy.processing_metrics['candles_processed'],
            'signals_generated': strategy.processing_metrics['signals_generated'],
            'win_rate': 0.6,  # Placeholder
            'max_drawdown_pct': 0,  # Placeholder
            'sharpe_ratio': 0,  # Placeholder
            'profit_factor': 1.0,  # Placeholder
            'indicators_calculated': strategy.processing_metrics['indicators_calculated'],
            'processing_time_avg': strategy.processing_metrics['avg_processing_time'],
            'errors': strategy.processing_metrics['errors']
        }
    
    async def _subscribe_to_strategy_signals(self, session_id: str, strategy: TradingStrategy):
        """S'abonne aux signaux d'une stratégie pour le tracking"""
        symbol = strategy.config.pairs[0] if strategy.config.pairs else "BTCUSD"
        
        signal_channel = CHANNELS.STRATEGIES_SIGNAL.format(
            strategyId=strategy.config.name,
            symbol=symbol
        )
        
        async def on_signal(signal_data: Dict, metadata):
            # Tracker les signaux pour les métriques globales
            self.global_metrics['total_signals_generated'] += 1
            
            # Log du signal
            signal = signal_data.get('signal', {})
            logger.info(f"Signal from {session_id}: {signal.get('type')} - {signal.get('rule_name')}")
        
        await self.pubsub.subscribe(signal_channel, on_signal)
    
    def get_active_strategies(self) -> List[str]:
        """Retourne la liste des stratégies actives"""
        return list(self.active_strategies.keys())
    
    def get_global_metrics(self) -> Dict:
        """Retourne les métriques globales"""
        # Ajouter les métriques du service d'indicateurs
        indicators_metrics = self.indicators_service.get_metrics()
        
        # Agrégation des métriques des stratégies
        total_candles = sum(
            strategy.processing_metrics['candles_processed'] 
            for strategy in self.active_strategies.values()
        )
        
        total_errors = sum(
            strategy.processing_metrics['errors']
            for strategy in self.active_strategies.values()
        )
        
        return {
            **self.global_metrics,
            'active_strategies': len(self.active_strategies),
            'total_candles_processed': total_candles,
            'total_errors': total_errors,
            'indicators_service_metrics': indicators_metrics,
            'uptime_seconds': (
                datetime.now(timezone.utc) - self.global_metrics['service_start_time']
            ).total_seconds() if self.global_metrics['service_start_time'] else 0
        }
    
    async def stop_all_strategies(self):
        """Arrête toutes les stratégies"""
        logger.info("Stopping all strategies...")
        
        stop_tasks = []
        for session_id in list(self.active_strategies.keys()):
            stop_tasks.append(self.stop_strategy(session_id))
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        logger.info("All strategies stopped")
    
    async def shutdown(self):
        """Arrête le manager et tous ses services"""
        await self.stop_all_strategies()
        await self.indicators_service.stop_service()
        
        logger.info("Enhanced Strategy Manager shutdown complete")