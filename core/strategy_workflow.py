"""
Strategy Workflow avec Queue et Interpréteur de Signaux
Architecture avec traitement séquentiel des candles et calculs parallèles d'indicateurs
"""

import asyncio
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timezone
from dataclasses import dataclass
import re
from uuid import uuid4
import os
from pathlib import Path

from data.models import StrategyConfig
from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from core.balance_tracker import BalanceTracker
from services.market_service import get_market_service
from strategies.performance_tracker import PerformanceTracker
#from indicators.indicators import IndicatorRegistry
from services.indicators_service import IndicatorRegistry

logger = logging.getLogger(__name__)

# Configurer le niveau de log depuis les variables d'environnement
import os
strategy_log_level = os.getenv("STRATEGY_LOG_LEVEL", "INFO").upper()
try:
    logger.setLevel(getattr(logging, strategy_log_level))
except AttributeError:
    logger.setLevel(logging.INFO)


@dataclass
class IndicatorConfig:
    """Configuration d'un indicateur"""
    type: str
    parameters: Dict
    source: str = "close"


@dataclass
class SignalRule:
    """Règle de signal avec condition interprétée"""
    name: str
    signal_type: str  # "LONG" ou "SHORT"
    condition: str    # Condition à évaluer
    priority: int = 1


@dataclass
class Signal:
    """Signal généré par une stratégie"""
    id: str
    strategy_id: str
    symbol: str
    type: str
    strength: int
    timestamp: float
    rule_name: str
    rule_condition: str
    dataframe_snapshot: Dict


class DataFrameConditionInterpreter:
    """Interpréteur de conditions basé sur les noms de colonnes du DataFrame"""
    
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe
        self.available_columns = set(dataframe.columns)
    
    def evaluate_condition(self, condition_str: str) -> bool:
        """
        Évalue une condition string basée sur les colonnes du DataFrame
        Exemples:
        - "EMA_50 > EMA_100"
        - "EMA_50[-1] < EMA_100[-1]" (valeur précédente)
        - "RSI_14 < 30"
        - "close > SMA_20 & SMA_20 > SMA_50"
        """
        try:
            # Remplacer les références de colonnes par les valeurs
            parsed_condition = self._parse_condition_string(condition_str)
            
            # Évaluer l'expression
            result = eval(parsed_condition)
            return bool(result)
            
        except Exception as e:
            logger.warning(f"Error evaluating condition '{condition_str}': {e}")
            return False
    
    def _parse_condition_string(self, condition_str: str) -> str:
        """Parse et remplace les références de colonnes par les valeurs"""
        # Pattern pour matcher les références de colonnes avec index optionnel
        # Ex: EMA_50, EMA_50[-1], close[0]
        # Éviter de matcher les nombres purs comme 70 ou 30
        column_pattern = r'([a-zA-Z]\w*(?:_\w+)*)((?:\[[-]?\d+\])?)'
        
        def replace_column_reference(match):
            column_name = match.group(1)
            index_part = match.group(2)
            
            if column_name not in self.available_columns:
                return "float('nan')"
            
            # Déterminer l'index
            if index_part:
                # Extraire l'index de [index]
                index_match = re.search(r'\[([-]?\d+)\]', index_part)
                if index_match:
                    index = int(index_match.group(1))
                else:
                    index = -1  # Dernière valeur par défaut
            else:
                index = -1  # Dernière valeur par défaut
            
            # Récupérer la valeur
            try:
                if index < 0:
                    # Index négatif (compter depuis la fin)
                    actual_index = len(self.df) + index
                else:
                    actual_index = index
                
                if actual_index < 0 or actual_index >= len(self.df):
                    return "float('nan')"  # Valeur invalide
                
                value = self.df.iloc[actual_index][column_name]
                
                if pd.isna(value):
                    return "float('nan')"
                
                return str(float(value))
                
            except (IndexError, KeyError):
                return "float('nan')"
        
        # Remplacer toutes les références de colonnes
        parsed = re.sub(column_pattern, replace_column_reference, condition_str)
        
        # Remplacer les opérateurs logiques
        parsed = parsed.replace('&', ' and ')
        parsed = parsed.replace('|', ' or ')
        parsed = parsed.replace('!', ' not ')
        
        return parsed


class TradingStrategy:
    """Stratégie avec queue de candles et traitement séquentiel"""
    
    def __init__(self, config: StrategyConfig, pubsub: PubSubEngine):
        self.config = config
        self.pubsub = pubsub
        
        # Queue pour traitement séquentiel des candles
        self.candle_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.processing_candle = False
        
        # DataFrame principal avec toutes les données
        self.dataframe = pd.DataFrame()
        self.dataframe_lock = asyncio.Lock()
        
        # Tracking des calculs d'indicateurs
        self.pending_indicators: Set[str] = set()
        self.indicators_results: Dict[str, List] = {}
        self.indicators_ready_event = asyncio.Event()
        
        # État de synchronisation
        self.last_processed_timestamp = 0
        self.current_candle_data = None
        
        # Worker de traitement
        self.candle_processor_task = None
        self.is_running = False
        
        # État de la stratégie - nouvelle architecture
        self.state = "idle"  # idle, ready, running, stopped
        
        # Optimisation cache - flag pour éviter retraitement historique
        self._cache_optimization_active = False
        
        # Configuration des indicateurs et règles
        self.indicators_config = getattr(config, 'indicators', {})
        self.signal_rules = getattr(config, 'signal_rules', [])
        
        # Performance tracking
        initial_balance = getattr(config, 'initial_balance', 100.0)
        self.performance_tracker = PerformanceTracker(config.name, initial_balance)
        
        # Balance tracking pour mise à jour temps réel
        self.balance_tracker = BalanceTracker(config.name, initial_balance)
        
        # Métriques de performance
        self.processing_metrics = {
            'candles_processed': 0,
            'signals_generated': 0,
            'indicators_calculated': 0,
            'avg_processing_time': 0.0,
            'errors': 0
        }
        self.indicators_registry = IndicatorRegistry()
    
    def _convert_timestamp_to_seconds(self, timestamp: float) -> float:
        """Convert timestamp to seconds, handling milliseconds automatically"""
        if timestamp > 10000000000:  # If timestamp > 2001, it's probably in milliseconds
            return timestamp / 1000
        return timestamp
    
    def _format_timestamp_to_date(self, timestamp: float) -> str:
        """Format timestamp to readable date string, handling milliseconds"""
        try:
            timestamp_seconds = self._convert_timestamp_to_seconds(timestamp)
            return datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OSError) as e:
            logger.warning(f"Invalid timestamp {timestamp}: {e}")
            return "Invalid Date"
    
    def _ensure_date_column(self):
        """Ensure date column exists and is up-to-date in the DataFrame"""
        if self.dataframe.empty or 'timestamp' not in self.dataframe.columns:
            return
        
        # Check if date column exists and has the right number of rows
        needs_date_column = (
            'date' not in self.dataframe.columns or 
            len(self.dataframe[self.dataframe['date'].isna()]) > 0
        )
        
        if needs_date_column:
            logger.debug(f"📅 Adding/updating date column for {len(self.dataframe)} rows")
            self.dataframe['date'] = self.dataframe['timestamp'].apply(self._format_timestamp_to_date)
            
            # Réorganiser les colonnes pour mettre date après timestamp
            cols = list(self.dataframe.columns)
            if 'timestamp' in cols and 'date' in cols:
                cols.remove('date')
                timestamp_idx = cols.index('timestamp')
                cols.insert(timestamp_idx + 1, 'date')
                self.dataframe = self.dataframe[cols]
    
    async def _check_cache_exists(self) -> bool:
        """Vérifie si un cache .feather existe pour cette stratégie"""
        try:
            symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'UNKNOWN')
            timeframe = getattr(self.config, 'timeframe', '1h')
            
            cache_dir = Path("cache_strategies") / self.config.name / self.config.id
            filename = f"{symbol}_{timeframe}.feather"
            filepath = cache_dir / filename
            
            exists = filepath.exists() and filepath.stat().st_size > 0
            logger.debug(f"🔍 Cache check: {filepath} exists={exists}")
            return exists
            
        except Exception as e:
            logger.debug(f"❌ Error checking cache: {e}")
            return False
    
    async def _publish_optimized_data_request(self, last_timestamp: float):
        """Publie une demande de données optimisée (seulement nouvelles chandelles)"""
        try:
            from datetime import datetime, timedelta
            
            # Calculer la nouvelle start_date depuis la dernière chandelle + 1 période
            last_timestamp_seconds = self._convert_timestamp_to_seconds(last_timestamp)
            last_date = datetime.fromtimestamp(last_timestamp_seconds)
            
            # Ajouter une période pour demander APRÈS la dernière chandelle
            timeframe = getattr(self.config, 'timeframe', '1h')
            timeframe_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                '1h': 3600, '4h': 14400, '1d': 86400
            }
            offset_seconds = timeframe_seconds.get(timeframe, 3600)
            
            # Start_date = dernière chandelle + 1 période
            next_candle_date = last_date + timedelta(seconds=offset_seconds)
            optimized_start_date = next_candle_date.strftime("%Y-%m-%d %H:%M:%S")
            optimized_historic = 0  # Pas d'historique, on a déjà tout
            original_end_date = getattr(self.config, 'end_date', 'now')  # PRÉSERVÉ
            
            logger.info(f"⚡ Publication optimisée:")
            logger.info(f"   📅 Dernière chandelle cache: {last_date.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"   📅 start_date optimisé: {optimized_start_date} (+{timeframe})")
            logger.info(f"   📊 historic: {optimized_historic} (pas d'historique)")
            logger.info(f"   🎯 end_date: {original_end_date} (config originale préservée)")
            
            # Publier avec paramètres optimisés
            symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'UNKNOWN')
            
            message = {
                "strategy_id": self.config.id,
                "symbol": symbol,
                "timeframe": timeframe,
                "start_date": optimized_start_date,  # Optimisé: APRÈS dernière chandelle
                "end_date": original_end_date,      # Original
                "historic": optimized_historic      # Optimisé
            }
            
            await self.pubsub.publish(f"market.data.request.{symbol}.{timeframe}", message)
            logger.info(f"📤 Demande optimisée publiée: nouvelles données depuis {optimized_start_date}")
            
        except Exception as e:
            logger.error(f"❌ Error publishing optimized request: {e}")
            # Fallback: publication normale
            await self._publish_normal_data_request()
    
    async def _publish_normal_data_request(self):
        """Publie une demande de données normale (historique complet)"""
        try:
            # Utiliser la config originale telle quelle
            symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'UNKNOWN')
            timeframe = getattr(self.config, 'timeframe', '1h')
            start_date = getattr(self.config, 'start_date', '2025-01-01')
            end_date = getattr(self.config, 'end_date', 'now')
            historic = getattr(self.config, 'historic', 200)
            
            logger.info(f"📂 Publication normale:")
            logger.info(f"   📅 start_date: {start_date}")
            logger.info(f"   📊 historic: {historic}")
            logger.info(f"   🎯 end_date: {end_date}")
            
            message = {
                "strategy_id": self.config.id,
                "symbol": symbol,
                "timeframe": timeframe,
                "start_date": start_date,
                "end_date": end_date,
                "historic": historic
            }
            
            await self.pubsub.publish(f"market.data.request.{symbol}.{timeframe}", message)
            logger.info(f"📤 Demande normale publiée: historique complet")
            
        except Exception as e:
            logger.error(f"❌ Error publishing normal request: {e}")
    
    async def start_strategy(self, load_from_cache: bool = False):
        """Démarre la stratégie - crée le worker et commence le traitement"""
        
        # Vérifier l'état de la stratégie
        if self.state == "ready":
            logger.info(f"🚀 Starting worker for strategy {self.config.name} (state: ready)")
        elif self.state == "idle":
            logger.info(f"🚀 Starting strategy {self.config.name} from scratch")
            
            # Optimisation intelligente - SANS MODIFIER LA CONFIG
            has_cache = await self._check_cache_exists()
            
            if has_cache:
                logger.info(f"💾 Cache détecté - Chargement optimisé du DataFrame")
                # Charger directement le DataFrame depuis le cache
                cache_last_timestamp = await self._load_dataframe_from_cache()
                
                if cache_last_timestamp:
                    logger.info(f"⚡ DataFrame chargé depuis cache - passant directement en mode ready")
                    # Passer directement en mode "ready" - éviter le traitement historique
                    self.state = "ready"
                    # Marquer que le cache a été utilisé pour optimisation
                    self._cache_optimization_active = True
                    
                    # NOUVELLE LOGIQUE: Ne pas demander de données si mode batch
                    end_date = getattr(self.config, 'end_date', 'now')
                    if end_date == 'now':
                        logger.info(f"🏁 Mode batch avec cache - Aucune demande de données, auto-stop immédiat")
                        # Auto-stop immédiat en mode batch - pas besoin de nouvelles données
                        self.state = "completed"
                        self._cache_optimization_active = False
                        return  # Sortir sans démarrer le worker
                    else:
                        # Mode live - demander nouvelles données seulement
                        logger.info(f"🔄 Mode live avec cache - Demande données après cache")
                        await self._publish_optimized_data_request(cache_last_timestamp)
                else:
                    # Cache invalide, traitement normal
                    logger.warning("⚠️ Cache invalide, basculement vers traitement normal")
                    self._cache_optimization_active = False
                    await self._publish_normal_data_request()
            else:
                logger.info(f"📂 Pas de cache - Chargement historique complet")
                # Publication normale avec config originale
                self._cache_optimization_active = False
                await self._publish_normal_data_request()
            
            # Initialiser les services si pas déjà fait
            await self._initialize_services()
        else:
            logger.warning(f"⚠️ Cannot start strategy in state: {self.state}")
            return
        
        # Marquer comme running et créer le worker
        self.is_running = True
        self.state = "running"
        
        # S'abonner aux données de marché seulement maintenant
        await self._request_market_data()
        
        # Démarrer le worker de traitement des candles
        self.candle_processor_task = asyncio.create_task(self._candle_processor_worker())
        
        logger.info(f"✅ Strategy {self.config.name} worker started (state: running)")
    
    async def _initialize_services(self):
        """Initialise les services sans créer de worker"""
        if hasattr(self, '_services_initialized'):
            return
            
        # S'abonner aux réponses du connecteur Kraken (nouveau pattern)
        await self._subscribe_to_candle_responses()
        
        # S'abonner aux services
        await self._subscribe_to_services()
        
        # S'abonner aux événements de queue pour monitoring
        await self._subscribe_to_queue_events()
        
        # Marquer comme initialisé
        self._services_initialized = True
        logger.debug(f"🔧 Services initialized for {self.config.name}")
    
    async def open_strategy(self):
        """Ouvre une stratégie existante depuis la dernière sauvegarde - SANS créer de worker"""
        logger.info(f"📂 Opening strategy {self.config.name} from cache...")
        
        try:
            # 1. Charger le DataFrame depuis le cache
            cache_last_timestamp = await self._load_dataframe_from_cache()
            
            if cache_last_timestamp:
                # 2. Ajuster automatiquement start_date selon la dernière chandelle
                await self._adjust_start_date_from_cache(cache_last_timestamp)
                logger.info(f"📈 DataFrame loaded with {len(self.dataframe)} rows")
                logger.info(f"🕐 Last candle timestamp: {cache_last_timestamp}")
            else:
                logger.info("📊 No cache found - will load historical data on start")
            
            # 3. Initialiser les services mais sans démarrer le worker
            await self._initialize_services()
            
            # 4. Définir l'état comme READY (prêt à démarrer, mais pas de worker actif)
            self.state = "ready"
            self.is_running = False  # Important : pas de worker encore
            
            logger.info(f"✅ Strategy {self.config.name} opened and ready (no worker created)")
            logger.info(f"💡 Use 'start_strategy()' to create worker and begin processing")
            
        except Exception as e:
            logger.error(f"❌ Error opening strategy {self.config.name}: {e}")
            self.state = "error"
            raise
    
    async def stop_strategy(self):
        """Arrête la stratégie et nettoie le worker"""
        logger.info(f"🛑 Stopping strategy {self.config.name} (current state: {self.state})...")
        
        # Arrêter immédiatement le flag de running avec tous les flags
        self.is_running = False
        self._stopping = True
        self.state = "stopped"
        
        # Réinitialiser l'optimisation cache
        self._cache_optimization_active = False
        
        logger.info(f"🔴 Set is_running=False for {self.config.name}")
        
        # Publier un message de désabonnement au market service
        try:
            await self._unsubscribe_from_market_data()
        except Exception as e:
            logger.warning(f"Error unsubscribing from market data: {e}")
        
        # Arrêter le worker de traitement
        if self.candle_processor_task : #and not self.candle_processor_task.done():
            logger.info(f"⏹️ Cancelling candle processor task for {self.config.name}")
            self.candle_processor_task.cancel()
            
            try:
                # Attendre l'arrêt avec timeout plus court
                await asyncio.wait_for(self.candle_processor_task, timeout=2.0)
                logger.info(f"✅ Candle processor task terminated for {self.config.name}")
            except asyncio.CancelledError:
                logger.info(f"✅ Candle processor task cancelled for {self.config.name}")
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Timeout cancelling worker for {self.config.name} - forcing termination")
                # Force termination plus agressive
                if self.candle_processor_task:
                    self.candle_processor_task.cancel()
            except Exception as e:
                logger.error(f"❌ Error stopping worker for {self.config.name}: {e}")
        else:
            logger.info(f"ℹ️ No candle processor task to cancel for {self.config.name}")
        
        # Vider la queue pour libérer la mémoire
        try:
            queue_size = self.candle_queue.qsize()
            cleared_items = 0
            while not self.candle_queue.empty():
                try:
                    self.candle_queue.get_nowait()
                    self.candle_queue.task_done()
                    cleared_items += 1
                except asyncio.QueueEmpty:
                    break
            if cleared_items > 0:
                logger.info(f"🧹 Cleared {cleared_items} items from queue for {self.config.name}")
        except Exception as e:
            logger.warning(f"Error clearing queue: {e}")
        
        # Forcer l'arrêt des publications PubSub en cours
        try:
            # Publier un message final d'arrêt
            stop_status = {
                "strategy_id": self.config.name,
                "event": "strategy_stopped",
                "timestamp": asyncio.get_event_loop().time(),
                "final_metrics": self.processing_metrics.copy()
            }
            await self.pubsub.publish(f"strategy.stopped.{self.config.name}", stop_status)
        except Exception as e:
            logger.warning(f"Error publishing stop status: {e}")
        
        logger.info(f"✅ Strategy {self.config.name} stopped completely")
    
    def force_stop(self):
        """Arrêt d'urgence synchrone de la stratégie"""
        logger.warning(f"🚨 Force stopping strategy {self.config.name}")
        self.is_running = False
        
        # Annuler la tâche brutalement
        if self.candle_processor_task and not self.candle_processor_task.done():
            self.candle_processor_task.cancel()
        
        # Vider la queue
        try:
            while not self.candle_queue.empty():
                try:
                    self.candle_queue.get_nowait()
                    self.candle_queue.task_done()
                except:
                    break
        except:
            pass
            
        logger.warning(f"🚨 Strategy {self.config.name} force stopped")
    
    async def _subscribe_to_candles(self):
        """S'abonne aux candles pour ce symbol/timeframe"""
        channel = CHANNELS.MARKET_DATA.format(
            symbol=self.config.pairs[0] if self.config.pairs else "BTCUSDC",  # Premier symbole
            timeframe=self.config.timeframe
        )
        await self.pubsub.subscribe(channel, self.on_new_candle, weak_ref=False)
        logger.info(f"Subscribed to candles: {channel}")
    
    async def _subscribe_to_candle_responses(self):
        """S'abonne aux réponses du connecteur Kraken (nouveau pattern)"""
        symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'BTCUSD')
        timeframe = getattr(self.config, 'timeframe', '5m')
        
        # Canal de réponse : market.data.response.<pair>.<tf>.<strategy_id>
        response_channel = f"market.data.response.{symbol}.{timeframe}.{self.config.id}"
        await self.pubsub.subscribe(response_channel, self.on_market_data_response, weak_ref=False)
        logger.info(f"✅ Subscribed to Kraken responses: {response_channel}")
        logger.info(f"🔧 Strategy config: id='{self.config.id}', name='{self.config.name}'")
    
    async def _request_market_data(self):
        """Publie une demande de données au connecteur Kraken"""
        symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'BTCUSD')
        timeframe = getattr(self.config, 'timeframe', '5m')
        
        # Canal de demande : market.data.request.<pair>.<tf>
        request_channel = f"market.data.request.{symbol}.{timeframe}"
        
        # Message de demande avec la stratégie émettrice
        request_data = {
            "strategy_id": self.config.id,
            "symbol": symbol,
            "timeframe": timeframe,
            "since": self.config.start_date,
            "to": self.config.end_date,
            "historic": self.config.historic,
            "exchange": getattr(self.config, 'exchange', 'kraken_future_spot'),
            "action": "request",
            "timestamp": datetime.now(timezone.utc).timestamp()
        }
        
        await self.pubsub.publish(request_channel, request_data)
        #candles_data = await get_market_service()._fetch_candles_data(symbol, timeframe, self.config.start_date, self.config.end_date, self.config.historic)

        logger.info(f"Published market data request: {request_channel}")
    
    async def _unsubscribe_from_market_data(self):
        """Publie une demande de désabonnement du market service"""
        try:
            symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'BTCUSD')
            timeframe = getattr(self.config, 'timeframe', '5m')
            
            # Canal de demande : market.data.request.<pair>.<tf>
            request_channel = f"market.data.request.{symbol}.{timeframe}"
            
            # Message de désabonnement
            request_data = {
                "strategy_id": self.config.id,
                "symbol": symbol,
                "timeframe": timeframe,
                "exchange": getattr(self.config, 'exchange', 'kraken_future_spot'),
                "action": "unsubscribe",
                "timestamp": datetime.now(timezone.utc).timestamp()
            }
            
            await self.pubsub.publish(request_channel, request_data)
            logger.info(f"Published market data unsubscribe request: {request_channel}")
            
        except Exception as e:
            logger.error(f"Error unsubscribing from market data: {e}")
    
    async def on_market_data_response(self, response_data: Dict, metadata):
        """Traite les réponses du connecteur Kraken avec DataFrame associée"""
        try:
            logger.info(f"🔥 on_market_data_response called for strategy {self.config.name}")
            logger.debug(f"Response data keys: {list(response_data.keys())}")
            
            # Vérifier que la réponse est pour cette stratégie
            response_strategy_id = response_data.get("strategy_id")
            if response_strategy_id != self.config.id:
                logger.debug(f"Response not for this strategy: {response_strategy_id} != {self.config.id}")
                return  # Pas pour nous
            
            logger.info(f"✅ Processing market data response for strategy {self.config.name}")
            
            # Extraire les données de chandelle et le DataFrame
            candles_data = response_data.get("candles", [])
            historic_data = response_data.get("historic", [])
            
            logger.info(f"📊 Received {len(candles_data)} candles and {len(historic_data) if historic_data else 0} historic data")
            
            # Traiter les données historiques d'abord (en tant que liste)
            if historic_data and len(historic_data) > 0:
                # OPTIMISATION: Skip historique si cache est chargé
                if hasattr(self, '_cache_optimization_active') and self._cache_optimization_active:
                    logger.info(f"⚡ OPTIMISATION: Skip {len(historic_data)} historic candles (cache chargé)")
                else:
                    logger.info(f"📈 Queuing {len(historic_data)} historic candles as batch")
                    await asyncio.wait_for(
                        self.candle_queue.put(historic_data),  # Envoyer la liste complète
                        timeout=2.0
                    )
            
            # Traiter les chandelles courantes individuellement (temps réel)
            if candles_data and len(candles_data) > 0:
                logger.info(f"📈 Queuing {len(candles_data)} current candles individually")
                for candle in candles_data:
                    await asyncio.wait_for(
                        self.candle_queue.put(candle),  # Traitement individuel pour temps réel
                        timeout=1.0
                    )
            
            logger.info(f"✅ All candles queued successfully for {self.config.name}")
                
        except asyncio.TimeoutError:
            logger.warning(f"Candle queue full for strategy {self.config.name}, dropping Kraken candle")
            self.processing_metrics['errors'] += 1
        except Exception as e:
            logger.error(f"Error processing Kraken candle response: {e}")
            self.processing_metrics['errors'] += 1
    
    async def _subscribe_to_services(self):
        """S'abonne aux services (indicateurs, balance, etc.)"""
        # Indicateurs - s'abonner aux résultats des indicateurs
        indicators_channel = f"indicators.result.{self.config.name}"
        await self.pubsub.subscribe(indicators_channel, self.on_indicators_update, weak_ref=False)
        
        # Balance updates
        balance_channel = CHANNELS.PORTFOLIO_UPDATE.format(userId="default")
        await self.pubsub.subscribe(balance_channel, self.on_balance_update, weak_ref=False)
        
        logger.info("Subscribed to services (indicators, balance)")
    
    async def _subscribe_to_queue_events(self):
        """S'abonne aux événements de queue pour monitoring externe"""
        # Autres services peuvent écouter ces canaux pour connaître l'état de la queue
        status_channel = f"strategy.status.{self.config.name}"
        completion_channel = f"strategy.batch.completed.{self.config.name}"
        
        # Optionnel: la stratégie peut aussi s'écouter elle-même pour des actions spéciales
        # await self.pubsub.subscribe(completion_channel, self.on_batch_completed, weak_ref=False)
        
        logger.debug(f"Queue event channels configured: {status_channel}, {completion_channel}")
    
    async def on_batch_completed(self, completion_data: Dict, metadata):
        """Appelé quand un batch est terminé (optionnel)"""
        try:
            # Actions à effectuer en fin de batch
            # Ex: sauvegarder état, nettoyer mémoire, mettre à jour UI, etc.
            
            metrics = completion_data.get("metrics", {})
            candles_processed = metrics.get("candles_processed", 0)
            
            logger.info(f"🎉 Batch completed: {candles_processed} candles processed")
            
            # Exemple d'actions de fin de batch:
            # - Sauvegarder l'état du DataFrame
            # - Envoyer notification à l'UI
            # - Déclencher une analyse de performance
            # - Nettoyer les données anciennes
            
        except Exception as e:
            logger.error(f"Error in batch completion handler: {e}")
    
    async def on_new_candle(self, candle_data: Dict, metadata):
        """Réception de nouvelle chandelle → ajout à la queue"""
        try:
            # Ajouter à la queue (non-bloquant avec timeout)
            await asyncio.wait_for(
                self.candle_queue.put(candle_data), 
                timeout=1.0
            )
            logger.debug(f"Candle queued for {self.config.name}: {candle_data.get('timestamp')}")
            
        except asyncio.TimeoutError:
            logger.warning(f"Candle queue full for strategy {self.config.name}, dropping candle")
            self.processing_metrics['errors'] += 1
        except Exception as e:
            logger.error(f"Error queuing candle: {e}")
            self.processing_metrics['errors'] += 1
    
    async def _candle_processor_worker(self):
        """Worker qui traite les candles séquentiellement avec support des listes et détection de queue vide"""
        queue_empty_timeout = 2.0  # Timeout plus court pour arrêt rapide
        last_queue_empty_check = 0
        
        logger.info(f"🚀 Candle processor worker started for {self.config.name}")
        
        while self.is_running:
            try:
                # Vérification rapide du flag avant toute opération
                if not self.is_running:
                    logger.info(f"🛑 is_running=False detected, breaking worker loop for {self.config.name}")
                    break
                
                # Attendre la prochaine chandelle avec timeout pour détecter queue vide
                try:
                    candle_data = await asyncio.wait_for(
                        self.candle_queue.get(), 
                        timeout=queue_empty_timeout
                    )
                    
                    # Double vérification après récupération des données
                    if not self.is_running:
                        logger.info(f"🛑 is_running=False detected after queue.get(), breaking for {self.config.name}")
                        break
                    
                except asyncio.TimeoutError:
                    # Queue vide détectée - vérifier si on doit encore tourner
                    if not self.is_running:
                        logger.info(f"🛑 is_running=False detected in timeout, breaking for {self.config.name}")
                        break
                    
                    # **NOUVELLE LOGIQUE** : Auto-stop en mode non-live quand queue vide
                    if self._should_auto_stop_on_empty_queue():
                        logger.info(f"🏁 Auto-stopping {self.config.name} - queue empty in non-live mode")
                        await self._auto_stop_strategy()
                        break
                        
                    # Éviter de spammer les événements de queue vide
                    current_time = asyncio.get_event_loop().time()
                    if current_time - last_queue_empty_check > queue_empty_timeout:
                        # Vérifier encore une fois avant de publier
                        if self.is_running:
                            await self._on_queue_empty()
                        last_queue_empty_check = current_time
                    continue
                
                # Mesurer le temps de traitement
                start_time = asyncio.get_event_loop().time()
                
                # Déterminer le type de traitement selon les données reçues
                if isinstance(candle_data, list):
                    # Traitement de liste de chandelles (historique)
                    #logger.info(f"📈 Processing {len(candle_data)} historical candles")
                    await self._process_multiple_candles(candle_data)
                else:
                    # Traitement d'une chandelle unique (temps réel)
                    await self._process_single_candle(candle_data)
                
                # Mettre à jour les métriques
                processing_time = asyncio.get_event_loop().time() - start_time
                self._update_processing_metrics(processing_time)
                
                # **NOUVEAU** : Tracker le temps de dernière chandelle traitée pour auto-stop
                self._last_candle_processed_time = asyncio.get_event_loop().time()
                
                # Marquer comme terminé
                self.candle_queue.task_done()
                
                # Vérifier si c'était le dernier élément de la queue
                if self.candle_queue.empty():
                    await self._on_queue_empty_immediate()
                
            except asyncio.CancelledError:
                # Worker annulé proprement
                logger.info(f"Candle processor worker cancelled for {self.config.name}")
                break
            except Exception as e:
                logger.error(f"Error in candle processor worker: {e}")
                self.processing_metrics['errors'] += 1
                await asyncio.sleep(1)  # Éviter la boucle d'erreurs
        
        logger.info(f"🛑 Candle processor worker stopped for {self.config.name}")
    
    def _should_auto_stop_on_empty_queue(self) -> bool:
        """Détermine si la stratégie doit s'arrêter automatiquement quand la queue est vide"""
        # Ne pas s'arrêter si on est en mode live (end_date = "live")
        if hasattr(self.config, 'end_date') and self.config.end_date == "live":
            return False
        
        # Ne pas s'arrêter si on vient de démarrer (moins de 10 secondes)
        if not hasattr(self, '_worker_start_time'):
            self._worker_start_time = asyncio.get_event_loop().time()
            return False
        
        current_time = asyncio.get_event_loop().time()
        if current_time - self._worker_start_time < 10.0:
            return False
        
        # Auto-stop en mode historique si :
        # 1. Queue vide depuis plus de 5 secondes
        # 2. On a traité au moins quelques chandelles
        # 3. end_date n'est pas "now"
        
        if not hasattr(self, '_last_candle_processed_time'):
            return False
        
        time_since_last_candle = current_time - self._last_candle_processed_time
        has_processed_candles = self.processing_metrics['candles_processed'] > 0
        
        logger.debug(f"Auto-stop check for {self.config.name}: "
                    f"time_since_last={time_since_last_candle:.1f}s, "
                    f"processed={self.processing_metrics['candles_processed']}, "
                    f"end_date={getattr(self.config, 'end_date', 'unknown')}")
        
        return (time_since_last_candle > 5.0 and has_processed_candles)
    
    async def _auto_stop_strategy(self):
        """Arrêt automatique propre de la stratégie"""
        logger.info(f"🔄 Auto-stopping strategy {self.config.name}")
        
        # Sauvegarder le DataFrame avant arrêt
        try:
            await self._save_dataframe_to_cache()
            logger.info(f"💾 DataFrame saved before auto-stop for {self.config.name}")
        except Exception as e:
            logger.warning(f"⚠️ Error saving DataFrame on auto-stop: {e}")
        
        # Marquer comme arrêté proprement (pas forcé)
        self.is_running = False
        self.state = "completed"  # État spécial pour arrêt automatique
        
        # Réinitialiser l'optimisation cache
        self._cache_optimization_active = False
        
        # Publier événement de completion
        try:
            completion_event = {
                "strategy_id": self.config.name,
                "event": "auto_completed",
                "reason": "queue_empty_non_live",
                "metrics": self.processing_metrics.copy(),
                "timestamp": asyncio.get_event_loop().time()
            }
            await self.pubsub.publish(f"strategy.completed.{self.config.name}", completion_event)
        except Exception as e:
            logger.warning(f"Error publishing completion event: {e}")
        
        logger.info(f"✅ Strategy {self.config.name} auto-stopped successfully")
    
    async def _on_queue_empty(self):
        """Appelé périodiquement quand la queue est vide (timeout)"""
        # Ne pas publier si la stratégie est en cours d'arrêt
        if not self.is_running:
            logger.debug(f"⏹️ Skipping queue empty publication for stopped strategy {self.config.name}")
            return
        
        # Compteur de répétitions pour détecter les boucles infinies
        if not hasattr(self, '_queue_empty_count'):
            self._queue_empty_count = 0
        self._queue_empty_count += 1
        
        # Kill switch : trop de répétitions = worker phantom
        if self._queue_empty_count > 20:  # Après 40 secondes (20 * 2s)
            logger.error(f"🚨 PHANTOM WORKER DETECTED - {self._queue_empty_count} consecutive queue empty calls")
            logger.error(f"🛑 Auto-terminating phantom worker for {self.config.name}")
            self.is_running = False
            self._force_stopped = True
            return
            
        current_time = asyncio.get_event_loop().time()
        
        # Publier statut de queue vide vers l'UI ou autres services
        queue_status = {
            "strategy_id": self.config.name,
            "queue_empty": True,
            "queue_size": self.candle_queue.qsize(),
            "last_processed": self.last_processed_timestamp,
            "processing_metrics": self.processing_metrics.copy(),
            "dataframe_size": len(self.dataframe),
            "timestamp": current_time
        }
        
        # Publier vers un canal de statut pour l'UI (logs réduits)
        try:
            # logger.debug(f"📈 Queue empty for {self.config.name}")  # Temporarily commented out to reduce spam
            await self.pubsub.publish(f"strategy.status.{self.config.name}", queue_status)
        except Exception as e:
            logger.warning(f"Error publishing queue status: {e}")
            # KILL SWITCH AUTOMATIQUE : Si erreur de publication ET aucun pattern trouvé  
            if "No matching patterns found" in str(e):
                logger.error(f"🚨 PHANTOM WORKER DETECTED - Auto-terminating {self.config.name}")
                logger.error(f"🔍 Error details: {e}")
                self.is_running = False
                self._force_stopped = True
                # Force arrêt immédiat
                return
    
    async def _on_queue_empty_immediate(self):
        """Appelé immédiatement quand la queue devient vide après traitement"""
        logger.info(f"📊 Queue emptied for {self.config.name} - "
                   f"Processed: {self.processing_metrics['candles_processed']}, "
                   f"Signals: {self.processing_metrics['signals_generated']}, "
                   f"DataFrame: {len(self.dataframe)} rows")
        
        # Sauvegarder le DataFrame pour analyse de performance
        await self._save_dataframe_to_cache()
        
        # Publier événement de fin de batch
        completion_status = {
            "strategy_id": self.config.name,
            "event": "batch_completed",
            "metrics": self.processing_metrics.copy(),
            "dataframe_info": self.get_dataframe_info(),
            "timestamp": asyncio.get_event_loop().time()
        }
        
        try:
            await self.pubsub.publish(f"strategy.batch.completed.{self.config.name}", completion_status)
        except Exception as e:
            logger.warning(f"Error publishing completion status: {e}")
        
        # **SOLUTION SIMPLE** : Arrêter automatiquement après traitement complet
        
        # Compter les completions pour détecter les boucles infinies
        if not hasattr(self, '_completion_count'):
            self._completion_count = 0
        self._completion_count += 1
        
        # DEBUG: Analyser la configuration end_date
        current_end_date = getattr(self.config, 'end_date', 'MISSING')
        has_end_date = hasattr(self.config, 'end_date')
        logger.info(f"🔍 DEBUG Auto-stop: end_date='{current_end_date}', hasattr={has_end_date}, completion#{self._completion_count}")
        
        # Si on n'est pas en mode live (end_date != "live"), arrêter le worker
        if hasattr(self.config, 'end_date') and self.config.end_date != "live":
            logger.info(f"🏁 Batch completed in non-live mode (end_date={self.config.end_date}) - auto-stopping worker for {self.config.name}")
            self.is_running = False
            self.state = "completed"
            logger.info(f"🛑 Worker auto-stopped: is_running={self.is_running}, state={self.state}")
        # Arrêt de sécurité : si trop de completions en mode live (worker phantom)
        elif self._completion_count > 3:
            logger.warning(f"🚨 Too many completions ({self._completion_count}) - stopping potential phantom worker for {self.config.name}")
            self.is_running = False
            self.state = "completed"
        else:
            logger.debug(f"💡 Batch completed in live mode (end_date=live) - worker continues for {self.config.name} (completion #{self._completion_count})")
    
    async def _save_dataframe_to_cache(self):
        """Sauvegarde le DataFrame en format Feather pour analyse de performance"""
        try:
            logger.info(f"💾 Attempting to save DataFrame for {self.config.name}")
            logger.info(f"📊 DataFrame shape: {self.dataframe.shape}")
            logger.info(f"📈 DataFrame empty: {self.dataframe.empty}")
            
            if self.dataframe.empty:
                logger.warning("❌ Empty DataFrame, skipping cache save - this explains why .feather files are empty!")
                logger.info(f"🔍 Processing metrics: {self.processing_metrics}")
                return
            
            # Déterminer la paire et timeframe
            symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'UNKNOWN')
            timeframe = getattr(self.config, 'timeframe', '1h')
            
            # Créer le chemin de cache : cache_strategies/strategy_name/strategy_id/SYMBOL_TIMEFRAME.feather
            cache_dir = Path("cache_strategies") / self.config.name / self.config.id
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Nom du fichier avec timestamp pour versioning
            timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{symbol}_{timeframe}.feather"
            filepath = cache_dir / filename
            
            # Fichier de backup avec timestamp
            backup_filename = f"{symbol}_{timeframe}_{timestamp_suffix}.feather"
            backup_filepath = cache_dir / backup_filename
            
            # Préparer DataFrame enrichi avec métadonnées
            df_to_save = self.dataframe.copy()
            
            # Ajouter métadonnées comme colonnes
            df_to_save['strategy_name'] = self.config.name
            df_to_save['strategy_id'] = self.config.id
            df_to_save['symbol'] = symbol
            df_to_save['timeframe'] = timeframe
            df_to_save['session_start'] = datetime.now().isoformat()
            df_to_save['candles_processed'] = self.processing_metrics['candles_processed']
            df_to_save['signals_generated'] = self.processing_metrics['signals_generated']
            df_to_save['indicators_calculated'] = self.processing_metrics['indicators_calculated']
            df_to_save['avg_processing_time'] = self.processing_metrics['avg_processing_time']
            df_to_save['errors'] = self.processing_metrics['errors']
            
            # Sauvegarder en format Feather (principal)
            df_to_save.to_feather(filepath)
            
            # Sauvegarder backup avec timestamp
            df_to_save.to_feather(backup_filepath)
            
            # Créer fichier de métadonnées JSON pour faciliter l'analyse
            metadata = {
                "strategy_name": self.config.name,
                "strategy_id": self.config.id,
                "symbol": symbol,
                "timeframe": timeframe,
                "session_start": datetime.now().isoformat(),
                "dataframe_shape": [len(df_to_save), len(df_to_save.columns)],
                "columns": list(df_to_save.columns),
                "metrics": self.processing_metrics.copy(),
                "config": {
                    "start_date": getattr(self.config, 'start_date', None),
                    "end_date": getattr(self.config, 'end_date', None),
                    "historic": getattr(self.config, 'historic', None),
                    "exchange": getattr(self.config, 'exchange', None)
                }
            }
            
            import json
            metadata_file = cache_dir / f"{symbol}_{timeframe}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"💾 DataFrame saved: {filepath} ({len(df_to_save)} rows, {len(df_to_save.columns)} cols)")
            logger.debug(f"Cache directory: {cache_dir}")
            
        except ImportError:
            logger.warning("pyarrow not available, cannot save DataFrame in Feather format")
        except Exception as e:
            logger.error(f"Error saving DataFrame to cache: {e}")
    
    async def _load_dataframe_from_cache(self) -> Optional[float]:
        """
        Charge le DataFrame depuis le cache s'il existe et retourne le timestamp de la dernière chandelle.
        Retourne None si aucun cache trouvé ou erreur.
        """
        try:
            # Déterminer la paire et timeframe
            symbol = self.config.pairs[0] if self.config.pairs else getattr(self.config, 'symbol', 'UNKNOWN')
            timeframe = getattr(self.config, 'timeframe', '1h')
            
            # Chemin vers le fichier de cache
            cache_dir = Path("cache_strategies") / self.config.name / self.config.id
            filename = f"{symbol}_{timeframe}.feather"
            filepath = cache_dir / filename
            
            if not filepath.exists():
                logger.info(f"📂 No cached DataFrame found at {filepath}")
                return None
            
            # Charger le DataFrame depuis le cache
            cached_df = pd.read_feather(filepath)
            
            if cached_df.empty:
                logger.warning("Cached DataFrame is empty")
                return None
            
            # Extraire les colonnes de données principales (exclure métadonnées)
            data_columns = ['timestamp', 'date', 'open', 'high', 'low', 'close', 'volume']
            available_data_columns = [col for col in data_columns if col in cached_df.columns]
            
            if not available_data_columns:
                logger.error("No valid data columns found in cached DataFrame")
                return None
            
            # Filtrer pour ne garder que les colonnes de données + indicateurs
            # (exclure les colonnes de métadonnées comme strategy_name, etc.)
            metadata_columns = ['strategy_name', 'strategy_id', 'symbol', 'timeframe', 
                               'session_start', 'candles_processed', 'signals_generated', 
                               'indicators_calculated', 'avg_processing_time', 'errors']
            
            data_df = cached_df.drop(columns=[col for col in metadata_columns if col in cached_df.columns], errors='ignore')
            
            # Restaurer le DataFrame principal
            async with self.dataframe_lock:
                self.dataframe = data_df.copy()
                # Utiliser la méthode centralisée pour s'assurer que la colonne date existe
                self._ensure_date_column()
            
            # Obtenir le timestamp de la dernière chandelle
            last_timestamp = None
            if 'timestamp' in self.dataframe.columns and not self.dataframe.empty:
                last_timestamp = float(self.dataframe['timestamp'].iloc[-1])
                self.last_processed_timestamp = last_timestamp
            
            # Restaurer les métriques si disponibles dans les métadonnées
            if 'candles_processed' in cached_df.columns:
                last_row = cached_df.iloc[-1]
                self.processing_metrics.update({
                    'candles_processed': int(last_row.get('candles_processed', 0)),
                    'signals_generated': int(last_row.get('signals_generated', 0)),
                    'indicators_calculated': int(last_row.get('indicators_calculated', 0)),
                    'avg_processing_time': float(last_row.get('avg_processing_time', 0)),
                    'errors': int(last_row.get('errors', 0))
                })
            
            logger.info(f"✅ DataFrame loaded from cache: {len(self.dataframe)} rows, last timestamp: {self._format_timestamp(last_timestamp) if last_timestamp else 'N/A'}")
            logger.info(f"📊 Restored metrics: {self.processing_metrics}")
            
            return last_timestamp
            
        except ImportError:
            logger.warning("pyarrow not available, cannot load DataFrame from Feather format")
            return None
        except Exception as e:
            logger.error(f"Error loading DataFrame from cache: {e}")
            return None
    
    async def _adjust_start_date_from_cache(self, last_timestamp: float):
        """
        Ajuste la start_date de la configuration pour reprendre depuis la dernière chandelle sauvegardée.
        """
        try:
            from datetime import datetime, timezone
            
            # Convertir le timestamp en date
            last_date = datetime.fromtimestamp(last_timestamp / 1000, timezone.utc)
            
            # Ajouter une période pour éviter le chevauchement
            # (ajouter 1h pour timeframe 1h, etc.)
            timeframe = getattr(self.config, 'timeframe', '1h')
            
            # Mapper les timeframes en secondes
            timeframe_seconds = {
                '1m': 60,
                '5m': 300,
                '15m': 900,
                '30m': 1800,
                '1h': 3600,
                '4h': 14400,
                '1d': 86400
            }
            
            # Obtenir les secondes correspondantes
            seconds_offset = timeframe_seconds.get(timeframe, 3600)  # Default 1h
            
            # Calculer la nouvelle start_date (dernière chandelle + 1 période)
            from datetime import timedelta
            adjusted_start_date = last_date + timedelta(seconds=seconds_offset)
            
            # Formater en string pour la configuration
            adjusted_start_str = adjusted_start_date.strftime('%Y-%m-%d %H:%M:%S')
            
            # Modifier dynamiquement la configuration
            if hasattr(self.config, 'start_date'):
                original_start_date = self.config.start_date
                self.config.start_date = adjusted_start_str
                
                logger.info(f"📅 Adjusted start_date from cache:")
                logger.info(f"  Original: {original_start_date}")
                logger.info(f"  Adjusted: {adjusted_start_str}")
                logger.info(f"  Last cached timestamp: {self._format_timestamp(last_timestamp)}")
            else:
                logger.warning("Config does not have start_date attribute, cannot adjust")
                
        except Exception as e:
            logger.error(f"Error adjusting start_date from cache: {e}")
    
    async def _process_multiple_candles(self, candles_list: List[Dict]):
        """Traite une liste de chandelles (historique) sans calculs d'indicateurs"""
        try:
            if not candles_list:
                logger.warning("Empty candles list received")
                return
            
            # OPTIMISATION: Si cache actif, ne pas traiter du tout l'historique
            if hasattr(self, '_cache_optimization_active') and self._cache_optimization_active:
                logger.info(f"⚡ OPTIMISATION: Skip complet de {len(candles_list)} chandelles historiques (cache chargé)")
                return
            
            #logger.info(f"🔄 Processing {len(candles_list)} historical candles without indicators")
            
            processed_count = 0
            skipped_count = 0
            
            # Traiter chaque chandelle en mode rapide (validation + ajout seulement)
            for i, candle_data in enumerate(candles_list):
                try:
                    # Validation et ajout au DataFrame seulement
                    if await self._validate_and_add_candle(candle_data):
                        processed_count += 1
                        if i % 50 == 0:  # Log progress tous les 50 candles
                            logger.debug(f"📊 Historical progress: {i+1}/{len(candles_list)} candles processed")
                    else:
                        skipped_count += 1
                        logger.debug(f"Skipped historical candle: {self._format_timestamp(candle_data.get('timestamp'))}")
                        
                except Exception as e:
                    logger.error(f"Error processing historical candle {i}: {e}")
                    skipped_count += 1
            
            # Mettre à jour les métriques
            self.processing_metrics['candles_processed'] += processed_count
            
            # S'assurer que la colonne date est présente après traitement par lots
            self._ensure_date_column()
            
            logger.info(f"✅ Historical candles processed: {processed_count} added, {skipped_count} skipped")
            logger.info(f"📊 DataFrame now contains {len(self.dataframe)} total rows")
            
        except Exception as e:
            logger.error(f"Error in process_multiple_candles: {e}")
            self.processing_metrics['errors'] += 1
    
    async def _process_single_candle(self, candle_data: Dict):
        """Traite une chandelle complète de bout en bout"""
        if self.processing_candle:
            logger.warning("Already processing a candle, skipping")
            return
        
        self.processing_candle = True
        self.current_candle_data = candle_data
        start_time = asyncio.get_event_loop().time()
        
        try:
            timestamp_str = self._format_timestamp(candle_data.get('timestamp'))
            #logger.info(f"🔄 Processing candle {timestamp_str}")
            
            # 1. Validation et ajout au DataFrame
            if not await self._validate_and_add_candle(candle_data):
                logger.warning("Candle validation failed, skipping")
                return
            
            # 2. Calculer tous les indicateurs en parallèle
            indicators_count = len(self.indicators_config) if self.indicators_config else 0
            indicators_time = 0
            
            if self.indicators_config:
                indicators_start = asyncio.get_event_loop().time()
                await self._calculate_indicators_parallel()
                await self._update_dataframe_with_indicators()
                indicators_time = asyncio.get_event_loop().time() - indicators_start
            
            # 3. Générer les signaux avec l'interpréteur
            signals_start = asyncio.get_event_loop().time()
            signals = await self._generate_signals_with_interpreter()
            signals_time = asyncio.get_event_loop().time() - signals_start
            
            # 4. Publier les signaux s'il y en a
            if signals:
                logger.info(f"🔄 Processing signals {signals}")

                #await self._publish_signals(signals)
                self.processing_metrics['signals_generated'] += len(signals)
            
            # Métriques finales
            total_time = asyncio.get_event_loop().time() - start_time
            self.processing_metrics['candles_processed'] += 1
            
            # Log de fin avec métriques détaillées
            indicators_success = len(self.indicators_results) if hasattr(self, 'indicators_results') else 0
            signals_generated = len(signals)
            
            #logger.info(f"✅ {timestamp_str}: {total_time:.3f}s | "
            #           f"Indicators: {indicators_success}/{indicators_count} ({indicators_time:.3f}s) | "
            #           f"Signals: {signals_generated} ({signals_time:.3f}s) | "
            #           f"DataFrame: {len(self.dataframe)} rows")
            
        except Exception as e:
            total_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"❌ Error processing candle {self._format_timestamp(candle_data.get('timestamp'))}: {e} (after {total_time:.3f}s)")
            self.processing_metrics['errors'] += 1
        finally:
            self.processing_candle = False
            self.current_candle_data = None
    
    async def _validate_and_add_candle(self, candle_data: Dict) -> bool:
        """Valide et ajoute la chandelle au DataFrame"""
        async with self.dataframe_lock:
            # Validation timestamp
            candle_timestamp = candle_data.get("timestamp", 0)
            logger.debug(f"🕐 Validating candle: timestamp={candle_timestamp}, last={self.last_processed_timestamp}")
            
            if candle_timestamp <= self.last_processed_timestamp:
                logger.debug(f"⏭️ Skipping candle: timestamp {candle_timestamp} <= last {self.last_processed_timestamp}")
                return False
            
            # NOUVEAU: Exclure chandelle courante non-close en mode batch
            if hasattr(self.config, 'end_date') and self.config.end_date == 'now':
                from datetime import datetime
                current_time = datetime.now().timestamp() * 1000  # ms
                candle_timestamp_ms = float(candle_timestamp)
                
                # Calculer le seuil selon le timeframe
                timeframe = getattr(self.config, 'timeframe', '1h')
                timeframe_ms = {
                    '1m': 60000,
                    '5m': 300000,
                    '15m': 900000,
                    '30m': 1800000,
                    '1h': 3600000,
                    '4h': 14400000,
                    '1d': 86400000
                }
                threshold_ms = timeframe_ms.get(timeframe, 3600000)  # Default 1h
                
                # Si la chandelle est trop récente (période courante non-close), la rejeter
                if current_time - candle_timestamp_ms < threshold_ms:
                    logger.debug(f"⏭️ Skipping non-closed candle in batch mode: {candle_timestamp} (too recent)")
                    return False
            
            # Créer nouvelle ligne avec données brutes de l'exchange (sans date)
            new_row = pd.DataFrame([{
                'timestamp': candle_timestamp,
                'open': float(candle_data.get('open', 0)),
                'high': float(candle_data.get('high', 0)), 
                'low': float(candle_data.get('low', 0)),
                'close': float(candle_data.get('close', 0)),
                'volume': float(candle_data.get('volume', 0))
            }])
            
            # Ajouter au DataFrame
            if self.dataframe.empty:
                logger.debug(f"📊 Creating new DataFrame with first candle")
                self.dataframe = new_row
            else:
                logger.debug(f"📈 Adding candle to existing DataFrame (current size: {len(self.dataframe)})")
                self.dataframe = pd.concat([self.dataframe, new_row], ignore_index=True)
            
            # Ajouter/mettre à jour la colonne date si elle n'existe pas ou est incomplète
            self._ensure_date_column()
            
            # Limiter la taille
            max_rows = getattr(self.config, 'max_dataframe_size', 10000)
            if len(self.dataframe) > max_rows:
                logger.debug(f"✂️ Trimming DataFrame from {len(self.dataframe)} to {max_rows} rows")
                self.dataframe = self.dataframe.tail(max_rows).reset_index(drop=True)
                # Recalculer la colonne date après trimming
                self._ensure_date_column()
            
            self.last_processed_timestamp = candle_timestamp
            logger.debug(f"✅ Candle added successfully. DataFrame now has {len(self.dataframe)} rows")
            return True
    
    async def _calculate_indicators_parallel(self):
        """Lance le calcul de tous les indicateurs en parallèle avec optimisations"""
        self.pending_indicators.clear()
        self.indicators_results.clear()
        self.indicators_ready_event.clear()
        
        if not self.indicators_config:
            self.indicators_ready_event.set()  # Marquer comme prêt immédiatement
            return
        
        # Préparer les données pour envoi
        async with self.dataframe_lock:
            if self.dataframe.empty:
                logger.warning("DataFrame is empty, cannot calculate indicators")
                self.indicators_ready_event.set()
                return
            dataframe_data = self.dataframe.to_dict('records')
        
        # Créer toutes les tâches de calcul en parallèle
        calculation_tasks = []
        
        for indicator_name, indicator_config in self.indicators_config.items():
            self.pending_indicators.add(indicator_name)
            
            # Créer tâche de calcul avec gestion d'erreur individuelle
            task = asyncio.create_task(
                self._safe_indicator_calculation(
                    indicator_name, 
                    indicator_config, 
                    self.dataframe
                ),
                name=f"indicator_{indicator_name}"
            )
            calculation_tasks.append(task)
        
        # Lancer tous les calculs en parallèle avec gestion d'erreurs
        try:
            results = await asyncio.gather(*calculation_tasks, return_exceptions=True)
            
            # Analyser les résultats
            successful_requests = 0
            failed_requests = 0
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_requests += 1
                    indicator_name = list(self.indicators_config.keys())[i]
                    logger.warning(f"Failed calculation for {indicator_name}: {result}")
                    # Retirer de pending pour éviter l'attente infinie
                    self.pending_indicators.discard(indicator_name)
                else:
                    indicator_name = list(self.indicators_config.keys())[i]
                    self.indicators_results[indicator_name] = result
                    successful_requests += 1
            
            # Si tous ont échoué, marquer comme prêt pour éviter le blocage
            if not self.pending_indicators:
                logger.warning("No pending indicators remaining, marking as ready")
                self.indicators_ready_event.set()
                
        except Exception as e:
            logger.error(f"Critical error in parallel indicator calculation: {e}")
            # En cas d'erreur critique, nettoyer et continuer
            self.pending_indicators.clear()
            self.indicators_ready_event.set()
    
    async def _safe_indicator_calculation(self, name: str, config: IndicatorConfig, df:pd.DataFrame):
        """Wrapper sécurisé pour la demande de calcul d'indicateur"""
        try:
            indicator_result = await asyncio.wait_for(
                self.indicators_registry.call_indicator(config.type, df, **config.parameters),
                timeout=5.0
            )
            return indicator_result
        except Exception as e:
            logger.warning(f"Error calculating {name}: {e}")
            # Retirer cet indicateur des pending pour éviter l'attente infinie
            self.pending_indicators.discard(name)
            raise

    async def _request_single_indicator_calculation(self, name: str, params: IndicatorConfig, dataframe_data: List[Dict]):
        """Demande le calcul d'un indicateur spécifique avec timeout"""
        try:
            # Utiliser un canal générique pour les demandes
            channel = "indicators.calculate"
            
            # Créer un request_id unique pour traçabilité
            request_id = f"{self.config.name}_{name}_{self._current_timestamp()}"
            
            # Préparer le message avec métadonnées enrichies
            #message_data = {
            #    "action": "calculate",
            #    "indicator": name,
            #    "config": config.__dict__ if hasattr(config, '__dict__') else config,
            #    "dataframe": dataframe_data,
            #    "strategy_id": self.config.name,
            #    "candle_timestamp": self.current_candle_data.get('timestamp') if self.current_candle_data else None,
            #    "request_id": request_id,
            #    "dataframe_size": len(dataframe_data),
            #    "timestamp": self._current_timestamp()
            #}
            #u
            # Publier avec timeout
            await asyncio.wait_for(
                self._calculate_indicator(indicator_type=name, df= dataframe_data, params=params.parameters),
                timeout=5.0
            )
            
            logger.debug(f"Published calculation request for {name} (request_id: {request_id})")
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout publishing calculation request for {name}")
            raise
        except Exception as e:
            logger.error(f"Error requesting calculation for {name}: {e}")
            raise

    async def _calculate_indicator(self, indicator_type: str, df: pd.DataFrame, parameters: Dict) -> List[
        float]:
        """Calcule un indicateur spécifique"""
        if indicator_type not in self.indicator_functions:
            raise ValueError(f"Unknown indicator type: {indicator_type}")

        calculator = self.indicator_functions[indicator_type]
        return await calculator(df, parameters)
    async def on_indicators_update(self, indicator_data: Dict, metadata):
        """Réception des résultats d'indicateurs avec optimisations parallèles"""
        try:
            strategy_id = indicator_data.get("strategy_id")
            if strategy_id != self.config.name:
                logger.debug(f"Indicator update not for this strategy: {strategy_id} != {self.config.name}")
                return
            
            # Vérifier que c'est pour la chandelle actuelle
            candle_timestamp = indicator_data.get("candle_timestamp")
            current_timestamp = self.current_candle_data.get('timestamp') if self.current_candle_data else None
            
            if current_timestamp and candle_timestamp != current_timestamp:
                logger.debug(f"Received indicator for old candle: {candle_timestamp} != {current_timestamp}, ignoring")
                return
            
            indicator_name = indicator_data.get("indicator")
            request_id = indicator_data.get("request_id", "unknown")
            
            if indicator_name in self.pending_indicators:
                # Stocker le résultat avec validation
                values = indicator_data.get("values", [])
                if values:
                    self.indicators_results[indicator_name] = values
                    logger.debug(f"✅ Received indicator {indicator_name} with {len(values)} values (request_id: {request_id})")
                else:
                    logger.warning(f"⚠️ Received empty values for indicator {indicator_name}")
                    self.indicators_results[indicator_name] = []
                
                # Retirer de la liste des pending
                self.pending_indicators.discard(indicator_name)
                
                logger.debug(f"📊 Indicator progress: {len(self.indicators_results)}/{len(self.indicators_config)} completed, {len(self.pending_indicators)} pending")
                
                # Vérifier si tous les indicateurs sont prêts
                if not self.pending_indicators:
                    logger.info(f"🎯 All {len(self.indicators_results)} indicators ready for parallel processing!")
                    self.indicators_ready_event.set()
            else:
                logger.debug(f"Received indicator {indicator_name} not in pending list: {list(self.pending_indicators)}")
                
        except Exception as e:
            logger.error(f"Error processing indicator update: {e}")
            # En cas d'erreur, continuer le traitement pour éviter le blocage
            indicator_name = indicator_data.get("indicator", "unknown")
            self.pending_indicators.discard(indicator_name)
    
    async def _wait_for_all_indicators(self, timeout: float = 15.0) -> bool:
        """Attend que tous les indicateurs soient calculés avec timeout optimisé"""
        try:
            # Si pas d'indicateurs configurés, retourner immédiatement
            if not self.indicators_config or not self.pending_indicators:
                logger.debug("No indicators to wait for, marking as ready")
                return True
            
            # Attendre avec timeout plus long pour les calculs parallèles
            start_time = asyncio.get_event_loop().time()
            logger.debug(f"Waiting for {len(self.pending_indicators)} indicators with {timeout}s timeout")
            
            await asyncio.wait_for(self.indicators_ready_event.wait(), timeout=timeout)
            
            elapsed_time = asyncio.get_event_loop().time() - start_time
            logger.info(f"✅ All indicators completed in {elapsed_time:.2f}s")
            return True
            
        except asyncio.TimeoutError:
            elapsed_time = asyncio.get_event_loop().time() - start_time
            missing_indicators = list(self.pending_indicators)
            logger.warning(f"⏰ Timeout after {elapsed_time:.2f}s waiting for indicators. Missing: {missing_indicators}")
            
            # Vérifier si en fait tous sont arrivés pendant l'attente
            if len(self.pending_indicators) == 0:
                logger.info("All indicators actually completed during timeout check")
                return True
            
            # Stratégie de fallback : continuer avec les indicateurs disponibles
            completed_indicators = [name for name in self.indicators_config.keys() if name in self.indicators_results]
            logger.info(f"Continuing with {len(completed_indicators)} completed indicators out of {len(self.indicators_config)}")
            
            return len(completed_indicators) > 0  # Continue s'il y a au moins un indicateur
    
    async def _update_dataframe_with_indicators(self):
        """Met à jour le DataFrame principal avec tous les indicateurs calculés en parallèle"""
        async with self.dataframe_lock:
            if self.dataframe.empty:
                return
            
            current_index = len(self.dataframe) - 1  # Index de la dernière ligne
            updated_indicators = 0
            failed_indicators = 0
            
            # Traiter tous les résultats d'indicateurs en parallèle
            for indicator_name, values in self.indicators_results.items():
                try:
                    # Vérifier si values est un DataFrame ou une Series
                    if isinstance(values, pd.DataFrame):
                        if not values.empty and len(values.columns) > 0:
                            # Pour un DataFrame, prendre la première colonne ou la colonne nommée
                            if indicator_name in values.columns:
                                column_values = values[indicator_name]
                            else:
                                # Prendre la première colonne
                                column_values = values.iloc[:, 0]
                            
                            if len(column_values) > 0:
                                last_value = column_values.iloc[-1]
                                
                                # Ajouter colonne si elle n'existe pas
                                if indicator_name not in self.dataframe.columns:
                                    self.dataframe[indicator_name] = np.nan
                                
                                if not pd.isna(last_value):
                                    self.dataframe.loc[current_index, indicator_name] = last_value
                                    updated_indicators += 1
                                else:
                                    failed_indicators += 1
                            else:
                                failed_indicators += 1
                        else:
                            failed_indicators += 1
                    
                    elif isinstance(values, pd.Series):
                        if not values.empty and len(values) > 0:
                            last_value = values.iloc[-1]
                            
                            # Ajouter colonne si elle n'existe pas
                            if indicator_name not in self.dataframe.columns:
                                self.dataframe[indicator_name] = np.nan
                            
                            if not pd.isna(last_value):
                                self.dataframe.loc[current_index, indicator_name] = last_value
                                updated_indicators += 1
                            else:
                                failed_indicators += 1
                        else:
                            failed_indicators += 1
                    
                    elif isinstance(values, (list, np.ndarray)):
                        if values and len(values) > 0:
                            last_value = values[-1]
                            
                            # Ajouter colonne si elle n'existe pas
                            if indicator_name not in self.dataframe.columns:
                                self.dataframe[indicator_name] = np.nan
                            
                            if not pd.isna(last_value):
                                self.dataframe.loc[current_index, indicator_name] = last_value
                                updated_indicators += 1
                            else:
                                failed_indicators += 1
                        else:
                            failed_indicators += 1
                    
                    else:
                        # Valeur scalaire
                        if not pd.isna(values):
                            # Ajouter colonne si elle n'existe pas
                            if indicator_name not in self.dataframe.columns:
                                self.dataframe[indicator_name] = np.nan
                            
                            self.dataframe.loc[current_index, indicator_name] = values
                            updated_indicators += 1
                        else:
                            failed_indicators += 1
                        
                except Exception as e:
                    logger.warning(f"Error updating {indicator_name}: {e}")
                    failed_indicators += 1
            
            # Mettre à jour les métriques
            self.processing_metrics['indicators_calculated'] += updated_indicators
    
    async def _generate_signals_with_interpreter(self) -> List[Signal]:
        """Génère les signaux en utilisant l'interpréteur de conditions"""
        signals = []
        
        async with self.dataframe_lock:
            # Vérifier qu'on a assez de données
            if len(self.dataframe) < 2:
                logger.debug("Not enough data for signal generation")
                return signals
            
            # Évaluer chaque règle avec l'interpréteur
            for rule in self.signal_rules:
                try:
                    interpreter = DataFrameConditionInterpreter(self.dataframe)
                    
                    if interpreter.evaluate_condition(rule.condition):
                        signal = Signal(
                            id=str(uuid4()),
                            strategy_id=self.config.name,
                            symbol=self.config.pairs[0] if self.config.pairs else "BTCUSD",
                            type=rule.signal_type,
                            strength=rule.priority,
                            timestamp=self._current_timestamp(),
                            rule_name=rule.name,
                            rule_condition=rule.condition,
                            dataframe_snapshot=self._get_dataframe_snapshot()
                        )
                        signals.append(signal)
                        
                        logger.info(f"Signal generated: {signal.type} by rule '{rule.name}': {rule.condition}")
                        
                except Exception as e:
                    logger.error(f"Error evaluating rule '{rule.name}': {e}")
            
            return signals
    
    def _get_dataframe_snapshot(self) -> Dict:
        """Récupère un snapshot des dernières valeurs du DataFrame"""
        if self.dataframe.empty:
            return {}
        
        last_row = self.dataframe.iloc[-1]
        return {k: (v if not pd.isna(v) else None) for k, v in last_row.to_dict().items()}
    
    async def _publish_signals(self, signals: List[Signal]):
        """Publie les signaux vers le service d'ordres"""
        for signal in signals:
            try:
                symbol = self.config.pairs[0] if self.config.pairs else "BTCUSD"
                channel = CHANNELS.STRATEGIES_SIGNAL.format(
                    strategyId=self.config.name,
                    symbol=symbol
                )
                
                await self.pubsub.publish(
                    channel,
                    {
                        "signal": {
                            "id": signal.id,
                            "strategy_id": signal.strategy_id,
                            "symbol": signal.symbol,
                            "type": signal.type,
                            "strength": signal.strength,
                            "timestamp": signal.timestamp,
                            "rule_name": signal.rule_name,
                            "rule_condition": signal.rule_condition,
                            "dataframe_snapshot": signal.dataframe_snapshot
                        },
                        "strategy_id": self.config.name,
                        "timestamp": self._current_timestamp(),
                        "dataframe_context": signal.dataframe_snapshot
                    }
                )
                
                logger.info(f"Published signal {signal.id} to order service")
                
            except Exception as e:
                logger.error(f"Error publishing signal {signal.id}: {e}")
    
    async def _request_balance_update(self):
        """Demande au service account de mettre à jour la balance dans le DataFrame"""
        try:
            await self.pubsub.publish(
                CHANNELS.PORTFOLIO_UPDATE.format(userId="default"),
                {
                    "action": "get_current_balance",
                    "strategy_id": self.config.name,
                    "candle_timestamp": self.current_candle_data.get('timestamp'),
                    "update_dataframe": True
                }
            )
            logger.debug("Requested balance update")
        except Exception as e:
            logger.error(f"Error requesting balance update: {e}")
    
    async def on_balance_update(self, balance_data: Dict, metadata):
        """Réception de mise à jour de balance → ajout au DataFrame"""
        strategy_id = balance_data.get("strategy_id")
        if strategy_id != self.config.name:
            return

        # Vérifier que c'est pour la chandelle actuelle
        candle_timestamp = balance_data.get("candle_timestamp")
        if self.current_candle_data and candle_timestamp != self.current_candle_data.get('timestamp'):
            return
        
        async with self.dataframe_lock:
            if not self.dataframe.empty:
                current_index = len(self.dataframe) - 1
                
                # Ajouter colonnes de balance si elles n'existent pas
                balance_columns = ['balance', 'equity', 'free_margin', 'margin_used']
                for col in balance_columns:
                    if col not in self.dataframe.columns:
                        self.dataframe[col] = np.nan
                
                # Mettre à jour avec les nouvelles valeurs
                self.dataframe.loc[current_index, 'balance'] = balance_data.get('balance', 0)
                self.dataframe.loc[current_index, 'equity'] = balance_data.get('equity', 0)
                self.dataframe.loc[current_index, 'free_margin'] = balance_data.get('free_margin', 0)
                self.dataframe.loc[current_index, 'margin_used'] = balance_data.get('margin_used', 0)
                
                logger.debug(f"Updated DataFrame with balance: {balance_data.get('balance')}")
    
    async def _publish_processing_metrics(self):
        """Publie les métriques de traitement"""
        try:
            metrics_data = {
                "strategy_id": self.config.name,
                "timestamp": self._current_timestamp(),
                "candles_processed": self.processing_metrics['candles_processed'],
                "signals_generated": self.processing_metrics['signals_generated'],
                "indicators_calculated": self.processing_metrics['indicators_calculated'],
                "avg_processing_time": self.processing_metrics['avg_processing_time'],
                "errors": self.processing_metrics['errors'],
                "queue_size": self.candle_queue.qsize(),
                "dataframe_rows": len(self.dataframe),
                "dataframe_columns": list(self.dataframe.columns) if not self.dataframe.empty else []
            }
            
            await self.pubsub.publish(
                f"strategies.metrics.{self.config.name}",
                metrics_data
            )
            
        except Exception as e:
            logger.error(f"Error publishing metrics: {e}")
    
    def _update_processing_metrics(self, processing_time: float):
        """Met à jour les métriques de performance"""
        # Moyenne mobile simple pour le temps de traitement
        if self.processing_metrics['avg_processing_time'] == 0:
            self.processing_metrics['avg_processing_time'] = processing_time
        else:
            self.processing_metrics['avg_processing_time'] = (
                self.processing_metrics['avg_processing_time'] * 0.9 + 
                processing_time * 0.1
            )
    
    def _current_timestamp(self) -> float:
        """Retourne le timestamp actuel"""
        return datetime.now(timezone.utc).timestamp()
    
    def _format_timestamp(self, timestamp: float) -> str:
        """Convertit un timestamp en date/heure lisible"""
        if not timestamp:
            return "no timestamp"
        
        try:
            if timestamp > 1e10:  # Timestamp en millisecondes
                dt = datetime.fromtimestamp(timestamp / 1000)
            else:  # Timestamp en secondes
                dt = datetime.fromtimestamp(timestamp)
            return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} (ts: {timestamp})"
        except (ValueError, OverflowError, OSError):
            return f"invalid timestamp: {timestamp}"
    
    def get_dataframe_info(self) -> Dict:
        """Retourne des informations sur le DataFrame pour debugging"""
        if self.dataframe.empty:
            return {"empty": True}
        
        return {
            "rows": len(self.dataframe),
            "columns": list(self.dataframe.columns),
            "last_timestamp": self.dataframe.iloc[-1]['timestamp'] if 'timestamp' in self.dataframe.columns else None,
            "last_values": self._get_dataframe_snapshot()
        }
    
    async def _update_balance_in_dataframe(self, candle_data: Dict):
        """Met à jour la balance courante dans le DataFrame après traitement de la chandelle"""
        try:
            timestamp = pd.Timestamp(candle_data.get('timestamp'))
            
            # Simuler un petit changement de balance (ici on pourrait intégrer le vrai calcul)
            # Pour la démo, on ajoute juste une entrée de balance
            current_balance = self.balance_tracker.current_balance
            
            # Ajouter l'entrée de balance pour cette chandelle
            self.balance_tracker.add_balance_entry(
                timestamp, 
                current_balance,
                0.0,  # Pas de trade fermé pour cette chandelle
                f"Fin traitement chandelle {timestamp}"
            )
            
            # Enrichir le DataFrame de la stratégie avec les données de balance
            self.dataframe = self.balance_tracker.add_to_strategy_dataframe(self.dataframe)
            
            logger.debug(f"Balance mise à jour: {current_balance}€ pour {timestamp}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour de balance: {e}")
    
    def get_current_balance(self) -> float:
        """Retourne la balance courante de la stratégie"""
        return self.balance_tracker.current_balance
    
    def get_balance_summary(self) -> Dict:
        """Retourne un résumé des performances de balance"""
        return self.balance_tracker.get_performance_summary()
    
    def get_queue_status(self) -> Dict:
        """Retourne le statut de la queue de traitement"""
        return {
            "queue_size": self.candle_queue.qsize(),
            "is_empty": self.candle_queue.empty(),
            "is_processing": self.processing_candle,
            "last_processed_timestamp": self.last_processed_timestamp,
            "processing_metrics": self.processing_metrics.copy(),
            "is_running": self.is_running
        }
    
    def is_queue_empty(self) -> bool:
        """Retourne True si la queue est vide"""
        return self.candle_queue.empty()
    
    async def wait_for_queue_empty(self, timeout: float = 30.0) -> bool:
        """Attend que la queue soit vide avec timeout"""
        try:
            start_time = asyncio.get_event_loop().time()
            while not self.candle_queue.empty() and self.is_running:
                await asyncio.sleep(0.1)
                if asyncio.get_event_loop().time() - start_time > timeout:
                    return False
            return True
        except Exception:
            return False