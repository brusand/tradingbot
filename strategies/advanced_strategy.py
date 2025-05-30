import asyncio
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, UTC
from dataclasses import dataclass
import re
import uuid

from strategies.base_strategy import BaseStrategy
from strategies.performance_tracker import PerformanceTracker, TradeRecord
from data.models import StrategyConfig
from connectors.kraken_connector import KrakenConnector


# Configuration des canaux PubSub
class CHANNELS:
    @staticmethod
    def INDICATORS_UPDATE(symbol: str, timeframe: str, indicator: str) -> str:
        return f"indicators.{symbol}.{timeframe}.{indicator}"
    
    @staticmethod
    def STRATEGIES_SIGNAL(strategyId: str, symbol: str) -> str:
        return f"strategies.{strategyId}.{symbol}.signals"
    
    @staticmethod
    def PORTFOLIO_UPDATE(userId: str) -> str:
        return f"portfolio.{userId}.update"


@dataclass
class IndicatorConfig:
    type: str
    parameters: Dict[str, Any]


@dataclass
class SignalRule:
    """Règle de signal avec interpréteur intégré"""
    name: str
    signal_type: str  # "LONG" ou "SHORT"
    condition: str   # String condition à évaluer
    priority: int = 1
    
    def evaluate(self, dataframe: pd.DataFrame) -> bool:
        """Évalue la règle avec l'interpréteur"""
        if len(dataframe) < 2:  # Besoin d'au moins 2 lignes pour [-1]
            return False
        
        interpreter = DataFrameConditionInterpreter(dataframe)
        return interpreter.evaluate_condition(self.condition)


@dataclass
class Signal:
    id: str
    strategy_id: str
    symbol: str
    type: str
    strength: int
    timestamp: datetime
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
            logging.debug(f"Condition '{condition_str}' → '{parsed_condition}' → {result}")
            return result
            
        except Exception as e:
            logging.error(f"Error evaluating condition '{condition_str}': {e}")
            return False
    
    def _parse_condition_string(self, condition_str: str) -> str:
        """Parse et remplace les références de colonnes par les valeurs"""
        # Pattern amélioré pour ne matcher que les vrais noms de colonnes
        # et éviter les nombres standalone
        column_pattern = r'\b(\w+(?:_\w+)*)((?:\[[-]?\d+\])?)'
        
        def replace_column_reference(match):
            column_name = match.group(1)
            index_part = match.group(2)
            
            # Skip si ce n'est pas une colonne valide (comme les nombres)
            if column_name not in self.available_columns:
                # Si c'est juste un nombre, le laisser tel quel
                if column_name.isdigit():
                    return match.group(0)
                # Sinon, ignorer silencieusement pour éviter les erreurs avec les constantes
                return match.group(0)
            
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


class AdvancedTradingStrategy(BaseStrategy):
    """Stratégie avancée avec queue de candles et interpréteur de signaux"""
    
    def __init__(self, config: StrategyConfig, connector: KrakenConnector):
        super().__init__(config, connector)
        
        # Queue pour traitement séquentiel des candles
        self.candle_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.processing_candle = False
        
        # DataFrame principal avec toutes les données
        self.dataframe = pd.DataFrame()
        self.dataframe_lock = asyncio.Lock()
        
        # Tracking des calculs d'indicateurs
        self.pending_indicators = set()
        self.indicators_results = {}
        self.indicators_ready_event = asyncio.Event()
        
        # État de synchronisation
        self.last_processed_timestamp = 0
        self.current_candle_data = None
        
        # Worker de traitement
        self.candle_processor_task = None
        
        # Configuration des indicateurs et règles
        self.indicators_config = self._setup_default_indicators()
        self.signal_rules = self._setup_default_signal_rules()
        
        # PubSub simulé (pour démonstration)
        self.pubsub = None  # Sera initialisé si nécessaire
        
        # Performance Tracker
        self.performance_tracker = PerformanceTracker(
            strategy_id=config.name,
            initial_capital=10000.0  # Capital initial par défaut
        )
        
        # Logging
        self.logger = logging.getLogger(f"AdvancedStrategy.{config.name}")
    
    def _setup_default_indicators(self) -> Dict[str, IndicatorConfig]:
        """Configuration par défaut des indicateurs"""
        return {
            "EMA_50": IndicatorConfig(type="EMA", parameters={"period": 50}),
            "EMA_100": IndicatorConfig(type="EMA", parameters={"period": 100}),
            "RSI_14": IndicatorConfig(type="RSI", parameters={"period": 14}),
            "SMA_20": IndicatorConfig(type="SMA", parameters={"period": 20}),
            "MACD": IndicatorConfig(type="MACD", parameters={"fast": 12, "slow": 26, "signal": 9})
        }
    
    def _setup_default_signal_rules(self) -> List[SignalRule]:
        """Configuration par défaut des règles de signaux"""
        return [
            # Signal LONG: EMA 50 croise au-dessus EMA 100 + RSI pas overbought
            SignalRule(
                name="ema_crossover_long",
                signal_type="LONG",
                condition="EMA_50 > EMA_100 & EMA_50[-1] <= EMA_100[-1] & RSI_14 < 70",
                priority=2
            ),
            
            # Signal SHORT: EMA 50 croise en-dessous EMA 100 + RSI pas oversold
            SignalRule(
                name="ema_crossover_short", 
                signal_type="SHORT",
                condition="EMA_50 < EMA_100 & EMA_50[-1] >= EMA_100[-1] & RSI_14 > 30",
                priority=2
            ),
            
            # Signal LONG simple: Prix au-dessus SMA + RSI oversold
            SignalRule(
                name="oversold_bounce",
                signal_type="LONG", 
                condition="close > SMA_20 & RSI_14 < 30 & RSI_14[-1] >= 30",
                priority=1
            ),
            
            # Signal SHORT simple: Prix en-dessous SMA + RSI overbought
            SignalRule(
                name="overbought_drop",
                signal_type="SHORT",
                condition="close < SMA_20 & RSI_14 > 70 & RSI_14[-1] <= 70",
                priority=1
            )
        ]
    
    async def initialize(self):
        """Initialise la stratégie avancée"""
        await super().initialize()
        
        # Démarrer le worker de traitement des candles
        self.candle_processor_task = asyncio.create_task(self._candle_processor_worker())
        
        self.logger.info(f"Advanced strategy initialized with queue processing")
    
    async def start(self):
        """Démarre la stratégie avec queue de traitement"""
        await super().start()
        # Note: super().start() already calls initialize()
        
        self.logger.info(f"Advanced strategy {self.config.name} started")
    
    async def stop(self):
        """Arrête la stratégie et nettoie les ressources"""
        await super().stop()
        
        if self.candle_processor_task:
            self.candle_processor_task.cancel()
            try:
                await self.candle_processor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info(f"Advanced strategy {self.config.name} stopped")
    
    async def on_market_data(self, symbol: str, data: Dict[str, Any]):
        """Réception de nouvelle chandelle → ajout à la queue"""
        try:
            # Simuler une chandelle OHLCV à partir des données de marché
            candle_data = {
                "timestamp": datetime.now(UTC).timestamp(),
                "open": data.get("price", 0),
                "high": data.get("price", 0),
                "low": data.get("price", 0),
                "close": data.get("price", 0),
                "volume": data.get("volume", 0)
            }
            
            # Ajouter à la queue (non-bloquant avec timeout)
            await asyncio.wait_for(
                self.candle_queue.put(candle_data), 
                timeout=1.0
            )
            self.logger.debug(f"Candle queued for {symbol}: {candle_data.get('timestamp')}")
            
        except asyncio.TimeoutError:
            self.logger.warning(f"Candle queue full, dropping candle")
        except Exception as e:
            self.logger.error(f"Error queuing candle: {e}")
    
    async def _candle_processor_worker(self):
        """Worker qui traite les candles séquentiellement"""
        while self.is_running:
            try:
                # Attendre la prochaine chandelle avec timeout
                candle_data = await asyncio.wait_for(
                    self.candle_queue.get(),
                    timeout=5.0
                )
                
                # Traiter la chandelle
                await self._process_single_candle(candle_data)
                
                # Marquer comme terminé
                self.candle_queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout normal, continuer
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in candle processor worker: {e}")
                await asyncio.sleep(1)  # Éviter la boucle d'erreurs
    
    async def _process_single_candle(self, candle_data: Dict):
        """Traite une chandelle complète de bout en bout"""
        if self.processing_candle:
            self.logger.warning("Already processing a candle, skipping")
            return
        
        self.processing_candle = True
        self.current_candle_data = candle_data
        
        try:
            # 1. Validation et ajout au DataFrame
            if not await self._validate_and_add_candle(candle_data):
                return
            
            # 2. Calculer tous les indicateurs
            await self._calculate_indicators_sync()
            
            # 3. Générer les signaux avec l'interpréteur
            signals = await self._generate_signals_with_interpreter()
            
            # 4. Traiter les signaux
            if signals:
                await self._process_signals(signals)
            
            # 5. Vérifier les conditions de sortie pour les trades ouverts
            self.simulate_exit_conditions()
            
            # 6. Mettre à jour les excursions des trades ouverts
            current_price = candle_data.get('close', 0)
            if current_price > 0:
                self.update_trade_excursions({self.config.pairs[0]: current_price})
            
            # 7. Publier métriques
            await self._publish_processing_metrics()
            
        except Exception as e:
            self.logger.error(f"Error processing candle {candle_data.get('timestamp')}: {e}")
        finally:
            self.processing_candle = False
            self.current_candle_data = None
    
    async def _validate_and_add_candle(self, candle_data: Dict) -> bool:
        """Valide et ajoute la chandelle au DataFrame"""
        async with self.dataframe_lock:
            # Validation timestamp
            candle_timestamp = candle_data.get("timestamp", 0)
            if candle_timestamp <= self.last_processed_timestamp:
                return False
            
            # Créer nouvelle ligne
            new_row = pd.DataFrame([{
                'timestamp': candle_timestamp,
                'open': candle_data['open'],
                'high': candle_data['high'], 
                'low': candle_data['low'],
                'close': candle_data['close'],
                'volume': candle_data['volume']
            }])
            
            # Ajouter au DataFrame
            if self.dataframe.empty:
                self.dataframe = new_row
            else:
                self.dataframe = pd.concat([self.dataframe, new_row], ignore_index=True)
            
            # Limiter la taille (garder les 1000 dernières chandelles)
            max_rows = 1000
            if len(self.dataframe) > max_rows:
                self.dataframe = self.dataframe.tail(max_rows).reset_index(drop=True)
            
            self.last_processed_timestamp = candle_timestamp
            return True
    
    async def _calculate_indicators_sync(self):
        """Calcule tous les indicateurs de manière synchrone (version simplifiée)"""
        async with self.dataframe_lock:
            if len(self.dataframe) < 2:
                return
            
            # Calculer EMA
            if 'EMA_50' not in self.dataframe.columns:
                self.dataframe['EMA_50'] = np.nan
            if 'EMA_100' not in self.dataframe.columns:
                self.dataframe['EMA_100'] = np.nan
            
            # Calcul EMA simplifié
            close_prices = self.dataframe['close']
            self.dataframe['EMA_50'] = close_prices.ewm(span=50, adjust=False).mean()
            self.dataframe['EMA_100'] = close_prices.ewm(span=100, adjust=False).mean()
            
            # Calculer SMA
            if 'SMA_20' not in self.dataframe.columns:
                self.dataframe['SMA_20'] = np.nan
            self.dataframe['SMA_20'] = close_prices.rolling(window=20).mean()
            
            # Calculer RSI simplifié
            if 'RSI_14' not in self.dataframe.columns:
                self.dataframe['RSI_14'] = np.nan
            
            delta = close_prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            self.dataframe['RSI_14'] = 100 - (100 / (1 + rs))
            
            self.logger.debug(f"Calculated indicators for {len(self.dataframe)} candles")
    
    async def _generate_signals_with_interpreter(self) -> List[Signal]:
        """Génère les signaux en utilisant l'interpréteur de conditions"""
        signals = []
        
        async with self.dataframe_lock:
            # Vérifier qu'on a assez de données
            if len(self.dataframe) < 2:
                return signals
            
            # Évaluer chaque règle avec l'interpréteur
            for rule in self.signal_rules:
                try:
                    if rule.evaluate(self.dataframe):
                        signal = Signal(
                            id=str(uuid.uuid4()),
                            strategy_id=self.config.name,
                            symbol=self.config.pairs[0] if self.config.pairs else "UNKNOWN",
                            type=rule.signal_type,
                            strength=rule.priority,
                            timestamp=datetime.now(UTC),
                            rule_name=rule.name,
                            rule_condition=rule.condition,
                            dataframe_snapshot=self._get_dataframe_snapshot()
                        )
                        signals.append(signal)
                        
                        self.logger.info(f"Signal generated: {signal.type} by rule '{rule.name}': {rule.condition}")
                        
                except Exception as e:
                    self.logger.error(f"Error evaluating rule '{rule.name}': {e}")
            
            return signals
    
    def _get_dataframe_snapshot(self) -> Dict:
        """Récupère un snapshot des dernières valeurs du DataFrame"""
        if self.dataframe.empty:
            return {}
        
        last_row = self.dataframe.iloc[-1]
        return last_row.to_dict()
    
    async def _process_signals(self, signals: List[Signal]):
        """Traite les signaux générés"""
        for signal in signals:
            try:
                # Logique de traitement des signaux
                await self._execute_signal(signal)
                
            except Exception as e:
                self.logger.error(f"Error processing signal {signal.id}: {e}")
    
    async def _execute_signal(self, signal: Signal):
        """Exécute un signal de trading"""
        # Calculer la taille de position
        current_price = signal.dataframe_snapshot.get('close', 0)
        position_size = await self.calculate_position_size(signal.symbol, current_price)
        
        if position_size <= 0:
            return
        
        # Générer un ID de trade unique
        trade_id = f"{signal.id}_{uuid.uuid4().hex[:8]}"
        
        # Placer l'ordre selon le type de signal
        try:
            if signal.type == "LONG":
                order_id = await self.place_buy_order(signal.symbol, position_size)
                if order_id:
                    # Enregistrer l'ouverture du trade
                    trade = self.performance_tracker.add_trade_entry(
                        trade_id=trade_id,
                        symbol=signal.symbol,
                        side="buy",
                        price=current_price,
                        quantity=position_size,
                        signal_id=signal.id,
                        signal_strength=signal.strength
                    )
                    trade.tags.append(signal.rule_name)
                    
                    self.logger.info(f"Opened LONG trade {trade_id} for signal {signal.id}: {position_size} @ {current_price}")
                
            elif signal.type == "SHORT":
                order_id = await self.place_sell_order(signal.symbol, position_size)
                if order_id:
                    # Enregistrer l'ouverture du trade
                    trade = self.performance_tracker.add_trade_entry(
                        trade_id=trade_id,
                        symbol=signal.symbol,
                        side="sell",
                        price=current_price,
                        quantity=position_size,
                        signal_id=signal.id,
                        signal_strength=signal.strength
                    )
                    trade.tags.append(signal.rule_name)
                    
                    self.logger.info(f"Opened SHORT trade {trade_id} for signal {signal.id}: {position_size} @ {current_price}")
                    
        except Exception as e:
            self.logger.error(f"Error executing signal {signal.id}: {e}")
    
    async def _publish_processing_metrics(self):
        """Publie les métriques de traitement"""
        metrics = {
            "dataframe_size": len(self.dataframe),
            "queue_size": self.candle_queue.qsize(),
            "processing_candle": self.processing_candle,
            "last_processed_timestamp": self.last_processed_timestamp,
            "available_columns": list(self.dataframe.columns) if not self.dataframe.empty else []
        }
        
        self.logger.debug(f"Processing metrics: {metrics}")
    
    async def should_buy(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Implémentation requise par BaseStrategy - utilise les signaux"""
        # Cette méthode est maintenant remplacée par le système de signaux
        return False
    
    async def should_sell(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Implémentation requise par BaseStrategy - utilise les signaux"""
        # Cette méthode est maintenant remplacée par le système de signaux
        return False
    
    async def calculate_position_size(self, symbol: str, price: float) -> float:
        """Calcule la taille de position pour un trade"""
        # Implémentation simple: position de test fixe
        return 0.01
    
    def get_dataframe_info(self) -> Dict:
        """Retourne des informations sur le DataFrame pour debug"""
        if self.dataframe.empty:
            return {"status": "empty"}
        
        return {
            "rows": len(self.dataframe),
            "columns": list(self.dataframe.columns),
            "latest_values": self._get_dataframe_snapshot(),
            "memory_usage": self.dataframe.memory_usage(deep=True).sum()
        }
    
    async def close_trade_by_id(self, trade_id: str, exit_price: float, reason: str = "manual") -> bool:
        """Ferme un trade spécifique"""
        trade = self.performance_tracker.close_trade(trade_id, exit_price)
        if trade:
            trade.tags.append(f"exit_{reason}")
            self.logger.info(f"Closed trade {trade_id}: {trade.pnl:.2f} PnL ({trade.pnl_pct:.2f}%)")
            return True
        return False
    
    async def close_all_trades(self, exit_price: float, reason: str = "strategy_stop") -> int:
        """Ferme tous les trades ouverts"""
        closed_count = 0
        open_trade_ids = list(self.performance_tracker.open_trades.keys())
        
        for trade_id in open_trade_ids:
            if await self.close_trade_by_id(trade_id, exit_price, reason):
                closed_count += 1
        
        return closed_count
    
    def update_trade_excursions(self, current_prices: Dict[str, float]):
        """Met à jour les excursions favorables/défavorables des trades ouverts"""
        for trade_id, trade in self.performance_tracker.open_trades.items():
            if trade.symbol in current_prices:
                self.performance_tracker.update_open_trade_excursion(
                    trade_id, current_prices[trade.symbol]
                )
    
    def get_performance_summary(self) -> Dict:
        """Retourne un résumé des performances de la stratégie"""
        return self.performance_tracker.export_summary_report()
    
    def get_trade_history(self) -> pd.DataFrame:
        """Retourne l'historique des trades"""
        return self.performance_tracker.get_trade_history_df()
    
    def get_equity_curve(self) -> pd.DataFrame:
        """Retourne la courbe d'équité"""
        return self.performance_tracker.get_equity_curve_df()
    
    def get_monthly_returns(self) -> pd.DataFrame:
        """Retourne les rendements mensuels"""
        return self.performance_tracker.get_monthly_returns()
    
    def get_performance_metrics(self):
        """Retourne les métriques de performance détaillées"""
        return self.performance_tracker.calculate_metrics()
    
    def simulate_exit_conditions(self):
        """Simule des conditions de sortie pour les trades ouverts (exemple)"""
        if self.dataframe.empty:
            return
        
        current_price = self.dataframe.iloc[-1]['close']
        current_rsi = self.dataframe.iloc[-1].get('RSI_14', 50)
        
        # Exemple de conditions de sortie
        trades_to_close = []
        
        for trade_id, trade in self.performance_tracker.open_trades.items():
            # Condition de stop loss (5%)
            if trade.side == "buy":
                loss_pct = ((current_price - trade.entry_price) / trade.entry_price) * 100
                if loss_pct < -5:  # Stop loss à -5%
                    trades_to_close.append((trade_id, "stop_loss"))
                elif current_rsi > 70:  # Take profit sur RSI overbought
                    trades_to_close.append((trade_id, "take_profit_rsi"))
            
            elif trade.side == "sell":
                loss_pct = ((trade.entry_price - current_price) / trade.entry_price) * 100
                if loss_pct < -5:  # Stop loss à -5%
                    trades_to_close.append((trade_id, "stop_loss"))
                elif current_rsi < 30:  # Take profit sur RSI oversold
                    trades_to_close.append((trade_id, "take_profit_rsi"))
        
        # Fermer les trades qui remplissent les conditions
        for trade_id, reason in trades_to_close:
            asyncio.create_task(self.close_trade_by_id(trade_id, current_price, reason))