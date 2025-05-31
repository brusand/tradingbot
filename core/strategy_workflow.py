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

from data.models import StrategyConfig
from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from core.balance_tracker import BalanceTracker
from strategies.performance_tracker import PerformanceTracker

logger = logging.getLogger(__name__)


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
            logger.debug(f"Parsed condition: '{condition_str}' -> '{parsed_condition}'")
            
            # Évaluer l'expression
            result = eval(parsed_condition)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error evaluating condition '{condition_str}': {e}")
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
                logger.warning(f"Column '{column_name}' not found in DataFrame. Available: {list(self.available_columns)}")
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
                
            except (IndexError, KeyError) as e:
                logger.debug(f"Error accessing {column_name}[{index}]: {e}")
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
        self.candle_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
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
        
        # Configuration des indicateurs et règles
        self.indicators_config = getattr(config, 'indicators', {})
        self.signal_rules = getattr(config, 'signal_rules', [])
        
        # Performance tracking
        initial_balance = getattr(config, 'initial_balance', 10000.0)
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
    
    async def start_strategy(self):
        """Démarre la stratégie avec queue de traitement"""
        self.is_running = True
        
        # S'abonner aux candles
        await self._subscribe_to_candles()
        
        # S'abonner aux services
        await self._subscribe_to_services()
        
        # Démarrer le worker de traitement des candles
        self.candle_processor_task = asyncio.create_task(self._candle_processor_worker())
        
        logger.info(f"Strategy {self.config.name} started with queue processing")
    
    async def stop_strategy(self):
        """Arrête la stratégie"""
        self.is_running = False
        
        if self.candle_processor_task:
            self.candle_processor_task.cancel()
            try:
                await self.candle_processor_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Strategy {self.config.name} stopped")
    
    async def _subscribe_to_candles(self):
        """S'abonne aux candles pour ce symbol/timeframe"""
        channel = CHANNELS.MARKET_DATA.format(
            symbol=self.config.pairs[0] if self.config.pairs else "BTCUSD",  # Premier symbole
            timeframe=self.config.timeframe
        )
        await self.pubsub.subscribe(channel, self.on_new_candle, weak_ref=False)
        logger.info(f"Subscribed to candles: {channel}")
    
    async def _subscribe_to_services(self):
        """S'abonne aux services (indicateurs, balance, etc.)"""
        # Indicateurs - s'abonner aux résultats des indicateurs
        indicators_channel = f"indicators.result.{self.config.name}"
        await self.pubsub.subscribe(indicators_channel, self.on_indicators_update, weak_ref=False)
        
        # Balance updates
        balance_channel = CHANNELS.PORTFOLIO_UPDATE.format(userId="default")
        await self.pubsub.subscribe(balance_channel, self.on_balance_update, weak_ref=False)
        
        logger.info("Subscribed to services (indicators, balance)")
    
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
        """Worker qui traite les candles séquentiellement"""
        while self.is_running:
            try:
                # Attendre la prochaine chandelle
                candle_data = await self.candle_queue.get()
                
                # Mesurer le temps de traitement
                start_time = asyncio.get_event_loop().time()
                
                # Traiter la chandelle
                await self._process_single_candle(candle_data)
                
                # Mettre à jour les métriques
                processing_time = asyncio.get_event_loop().time() - start_time
                self._update_processing_metrics(processing_time)
                
                # Marquer comme terminé
                self.candle_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in candle processor worker: {e}")
                self.processing_metrics['errors'] += 1
                await asyncio.sleep(1)  # Éviter la boucle d'erreurs
    
    async def _process_single_candle(self, candle_data: Dict):
        """Traite une chandelle complète de bout en bout"""
        if self.processing_candle:
            logger.warning("Already processing a candle, skipping")
            return
        
        self.processing_candle = True
        self.current_candle_data = candle_data
        
        try:
            logger.debug(f"Processing candle {candle_data.get('timestamp')}")
            
            # 1. Validation et ajout au DataFrame
            if not await self._validate_and_add_candle(candle_data):
                logger.debug("Candle validation failed, skipping")
                return
            
            # 2. Calculer tous les indicateurs en parallèle
            if self.indicators_config:
                await self._calculate_indicators_parallel()
                
                # 3. Attendre que tous les indicateurs soient prêts
                indicators_completed = await self._wait_for_all_indicators(timeout=10.0)
                
                if not indicators_completed:
                    logger.warning(f"Indicators timeout for candle {candle_data.get('timestamp')}")
                    return
                
                # 4. Mettre à jour le DataFrame avec les indicateurs
                await self._update_dataframe_with_indicators()
            
            # 5. Générer les signaux avec l'interpréteur
            signals = await self._generate_signals_with_interpreter()
            
            # 6. Publier les signaux s'il y en a
            if signals:
                await self._publish_signals(signals)
                self.processing_metrics['signals_generated'] += len(signals)
            
            # 7. Mettre à jour la balance dans le DataFrame
            await self._update_balance_in_dataframe(candle_data)
            
            # 8. Demander mise à jour de la balance
            await self._request_balance_update()
            
            # 9. Publier métriques
            await self._publish_processing_metrics()
            
            self.processing_metrics['candles_processed'] += 1
            logger.debug(f"Successfully processed candle {candle_data.get('timestamp')}")
            
        except Exception as e:
            logger.error(f"Error processing candle {candle_data.get('timestamp')}: {e}")
            self.processing_metrics['errors'] += 1
        finally:
            self.processing_candle = False
            self.current_candle_data = None
    
    async def _validate_and_add_candle(self, candle_data: Dict) -> bool:
        """Valide et ajoute la chandelle au DataFrame"""
        async with self.dataframe_lock:
            # Validation timestamp
            candle_timestamp = candle_data.get("timestamp", 0)
            if candle_timestamp <= self.last_processed_timestamp:
                logger.debug(f"Old candle timestamp {candle_timestamp} <= {self.last_processed_timestamp}")
                return False
            
            # Créer nouvelle ligne
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
                self.dataframe = new_row
            else:
                self.dataframe = pd.concat([self.dataframe, new_row], ignore_index=True)
            
            # Limiter la taille
            max_rows = getattr(self.config, 'max_dataframe_size', 1000)
            if len(self.dataframe) > max_rows:
                self.dataframe = self.dataframe.tail(max_rows).reset_index(drop=True)
            
            self.last_processed_timestamp = candle_timestamp
            logger.debug(f"Added candle to DataFrame. Total rows: {len(self.dataframe)}")
            return True
    
    async def _calculate_indicators_parallel(self):
        """Lance le calcul de tous les indicateurs en parallèle"""
        self.pending_indicators.clear()
        self.indicators_results.clear()
        self.indicators_ready_event.clear()
        
        # Préparer les données pour envoi
        async with self.dataframe_lock:
            dataframe_data = self.dataframe.to_dict('records')
        
        # Lancer tous les calculs en parallèle
        calculation_tasks = []
        
        for indicator_name, indicator_config in self.indicators_config.items():
            self.pending_indicators.add(indicator_name)
            
            # Créer tâche de calcul
            task = asyncio.create_task(
                self._request_single_indicator_calculation(
                    indicator_name, 
                    indicator_config, 
                    dataframe_data
                )
            )
            calculation_tasks.append(task)
        
        # Attendre que toutes les demandes soient envoyées
        await asyncio.gather(*calculation_tasks, return_exceptions=True)
        
        logger.debug(f"Requested calculation for {len(self.pending_indicators)} indicators")
    
    async def _request_single_indicator_calculation(self, name: str, config: IndicatorConfig, dataframe_data: List[Dict]):
        """Demande le calcul d'un indicateur spécifique"""
        try:
            # Utiliser un canal générique pour les demandes
            channel = "indicators.calculate"
            
            await self.pubsub.publish(
                channel,
                {
                    "action": "calculate",
                    "indicator": name,
                    "config": config.__dict__ if hasattr(config, '__dict__') else config,
                    "dataframe": dataframe_data,
                    "strategy_id": self.config.name,
                    "candle_timestamp": self.current_candle_data.get('timestamp'),
                    "request_id": f"{self.config.name}_{name}_{self._current_timestamp()}"
                }
            )
            logger.info(f"Published calculation request for {name} on channel {channel}")
            
        except Exception as e:
            logger.error(f"Error requesting calculation for {name}: {e}")
    
    async def on_indicators_update(self, indicator_data: Dict, metadata):
        """Réception des résultats d'indicateurs"""
        strategy_id = indicator_data.get("strategy_id")
        if strategy_id != self.config.name:
            return
        
        # Vérifier que c'est pour la chandelle actuelle
        candle_timestamp = indicator_data.get("candle_timestamp")
        if self.current_candle_data and candle_timestamp != self.current_candle_data.get('timestamp'):
            logger.debug(f"Received indicator for old candle, ignoring")
            return
        
        indicator_name = indicator_data.get("indicator")
        if indicator_name in self.pending_indicators:
            # Stocker le résultat
            self.indicators_results[indicator_name] = indicator_data.get("values", [])
            self.pending_indicators.discard(indicator_name)
            
            logger.debug(f"Received indicator {indicator_name}, pending: {len(self.pending_indicators)}")
            
            # Vérifier si tous les indicateurs sont prêts
            if not self.pending_indicators:
                self.indicators_ready_event.set()
                logger.debug("All indicators ready!")
    
    async def _wait_for_all_indicators(self, timeout: float = 10.0) -> bool:
        """Attend que tous les indicateurs soient calculés"""
        try:
            await asyncio.wait_for(self.indicators_ready_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for indicators. Missing: {self.pending_indicators}")
            return len(self.pending_indicators) == 0  # Continue si tous reçus pendant l'attente
    
    async def _update_dataframe_with_indicators(self):
        """Met à jour le DataFrame principal avec tous les indicateurs"""
        async with self.dataframe_lock:
            current_index = len(self.dataframe) - 1  # Index de la dernière ligne
            
            for indicator_name, values in self.indicators_results.items():
                if values and len(values) > 0:
                    # Ajouter colonne si elle n'existe pas
                    if indicator_name not in self.dataframe.columns:
                        self.dataframe[indicator_name] = np.nan
                    
                    # Mettre à jour avec la dernière valeur
                    self.dataframe.loc[current_index, indicator_name] = values[-1]
            
            self.processing_metrics['indicators_calculated'] += len(self.indicators_results)
            logger.debug(f"DataFrame updated with {len(self.indicators_results)} indicators")
    
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