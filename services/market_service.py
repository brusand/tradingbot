#!/usr/bin/env python3
"""
Service Kraken Market Data
Écoute les demandes sur market.data.<pair>.<tf> 
Répond sur market.data.<pair>.<tf>.candles avec stratégie associée et DataFrame
"""

import asyncio
import logging
import sys
import os

import click
import pandas as pd
from typing import Dict, Set, Optional
from datetime import datetime, timezone

from core.candles_fetcher import CandlesFetcher

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from interfaces.connector import Connector
from core.pubsub_engine import PubSubEngine, global_pubsub

logger = logging.getLogger(__name__)


class MarketService:
    """Service qui gère les demandes de données de marché via PubSub"""
    
    def __init__(self,  pubsub: PubSubEngine, connector: Connector):
        self.pubsub = pubsub or global_pubsub
        self.active_subscriptions: Dict[str, Dict] = {}
        self.is_running = False
        self.fetch_tasks: Dict[str, asyncio.Task] = {}
        
        # Stockage des DataFrames par symbole/timeframe
        self.dataframes: Dict[str, pd.DataFrame] = {}
        self.connector = connector
        # Métriques
        self.metrics = {
            "requests_received": 0,
            "candles_fetched": 0,
            "responses_sent": 0,
            "errors": 0
        }
        self.candles_fetcher = CandlesFetcher(connector, cache_dir="cache_crypto")
        self.candles_fetcher_initialized = False
    
    async def start(self):
        """Démarre le service"""
        self.is_running = True
        
        # Initialiser le CandlesFetcher avec ses connecteurs
        if not self.candles_fetcher_initialized:
            await self.candles_fetcher.__aenter__()
            self.candles_fetcher_initialized = True
            logger.info("✅ CandlesFetcher initialized with connectors")
        
        # S'abonner au pattern de demandes : market.data.*
        # Utilisation d'un wildcard pattern pour capturer toutes les demandes
        await self._subscribe_to_requests()
        
        logger.info("✅ Market Service started")
    
    async def stop(self):
        """Arrête le service"""
        self.is_running = False
        
        # Arrêter toutes les tâches de fetch
        for task in self.fetch_tasks.values():
            task.cancel()
        
        # Attendre l'arrêt
        await asyncio.gather(*self.fetch_tasks.values(), return_exceptions=True)
        
        # Fermer le CandlesFetcher et ses connecteurs
        if self.candles_fetcher_initialized:
            await self.candles_fetcher.__aexit__(None, None, None)
            self.candles_fetcher_initialized = False
            logger.info("🛑 CandlesFetcher stopped")
        
        logger.info("🛑 Kraken Market Service stopped")
    
    async def _subscribe_to_requests(self):
        """S'abonne aux canaux de demandes de données"""
        # S'abonner aux demandes mais pas aux réponses
        # On utilise une approche plus spécifique pour éviter de recevoir nos propres réponses

        request_channel = f"market.data.request.*"
        await self.pubsub.subscribe(
            request_channel,
            self.on_market_data_request,
            weak_ref=False
        )
        
        logger.info(f"Subscribed to {request_channel} request channels")
    
    async def on_market_data_request(self, request_data: Dict, metadata):
        """Traite une demande de données de marché"""
        try:
            logger.info(f"🔥 MARKET: on_market_data_request called with data: {request_data}")
            print(f"🔥 MARKET CALLBACK CALLED! Data: {request_data}")  # Test avec print au lieu de click.echo
            self.metrics["requests_received"] += 1
            
            # Remplacer click.echo par logger pour éviter les blocages
            logger.info("on_market_data_request received")
            strategy_id = request_data.get("strategy_id")
            symbol = request_data.get("symbol")
            timeframe = request_data.get("timeframe")
            since = request_data.get("since")
            to = request_data.get("to")
            historic = request_data.get("historic")
            action = request_data.get("action", "request")
            
            if not all([strategy_id, symbol, timeframe]):
                logger.warning(f"Invalid request: missing required fields. strategy_id={strategy_id}, symbol={symbol}, timeframe={timeframe}")
                return
            
            subscription_key = f"{symbol}.{timeframe}.{strategy_id}"
            
            if action == "request":
                # Requête ponctuelle - exécution immédiate
                await self._handle_request(strategy_id, symbol, timeframe, since, to, historic)
            elif action == "subscribe":
                # Abonnement continu - worker persistant + websocket live
                await self._handle_subscription(strategy_id, symbol, timeframe, subscription_key, since, to, historic)
            elif action == "unsubscribe":
                await self._handle_unsubscription(strategy_id, symbol, timeframe, subscription_key)
            
        except Exception as e:
            logger.error(f"Error processing market data request: {e}")
            self.metrics["errors"] += 1
    
    async def _handle_subscription(self, strategy_id: str, symbol: str, timeframe: str, subscription_key: str, since: str, to: str, historic: int=200):
        """Gère une demande d'abonnement avec worker live"""
        
        # Ajouter la stratégie aux abonnements actifs
        if subscription_key not in self.active_subscriptions:
            self.active_subscriptions[subscription_key] = {
                "symbol": symbol,
                "timeframe": timeframe,
                "strategies": set(),
                "last_candle_timestamp": 0,
                "since": since,
                "to": to,
                "historic": historic,
                "subscription_type": "live"  # Marquer comme abonnement live
            }
        
        self.active_subscriptions[subscription_key]["strategies"].add(strategy_id)
        
        logger.info(f"✅ Live subscription added: {strategy_id} -> {subscription_key}")
        
        # Démarrer le worker live si pas déjà actif
        if subscription_key not in self.fetch_tasks:
            logger.info(f"🚀 Creating live subscription worker for {subscription_key}")
            
            # Créer le worker avec gestion d'erreur
            async def wrapped_live_worker():
                try:
                    await self._live_subscription_worker(symbol, timeframe, subscription_key, since, to, historic)
                except Exception as e:
                    logger.error(f"💥 LIVE WORKER CRASHED for {subscription_key}: {e}")
                    print(f"💥 LIVE WORKER CRASHED for {subscription_key}: {e}")
                    import traceback
                    logger.error(f"💥 Traceback: {traceback.format_exc()}")
                    print(f"💥 Traceback: {traceback.format_exc()}")
            
            # Créer la tâche de manière non-bloquante
            self.fetch_tasks[subscription_key] = asyncio.create_task(wrapped_live_worker())
            logger.info(f"🚀 Live worker task created for {subscription_key}")
            
        else:
            logger.info(f"📋 Live worker already exists for {subscription_key}")
        
        # Envoyer d'abord les données historiques si demandées
        if historic > 0:
            logger.info(f"📊 Sending historical data ({historic} candles) for {strategy_id}")
            await self._send_historical_data(strategy_id, symbol, timeframe, since, to, historic)
            
        logger.info(f"🔥 Live subscription handled successfully for {strategy_id} -> {subscription_key}")
    
    async def _handle_request(self, strategy_id: str, symbol: str, timeframe: str, since: str, to: str, historic: int = 200):
        """Gère une demande ponctuelle de données (pas d'abonnement)"""
        try:
            logger.info(f"📋 Processing one-time request for {strategy_id}: {symbol}.{timeframe}")
            
            # Exécuter directement la requête ponctuelle (bloquant pour cette demande)
            await self._execute_single_request(strategy_id, symbol, timeframe, since, to, historic)
            
            logger.info(f"✅ One-time request completed for {strategy_id}")
            
        except Exception as e:
            logger.error(f"Error handling request for {strategy_id}: {e}")
            self.metrics["errors"] += 1
    
    async def _execute_single_request(self, strategy_id: str, symbol: str, timeframe: str, since: str, to: str, historic: int):
        """Exécute une requête ponctuelle et envoie la réponse"""
        try:
            logger.info(f"🔄 Executing single request for {strategy_id}: {symbol}.{timeframe}")
            
            # Récupérer les données
            candles_data = await self._fetch_candles_data(symbol, timeframe, since, to, historic)
            
            if candles_data:
                # Envoyer la réponse directement
                await self._send_candle_response(strategy_id, symbol, timeframe, candles_data)
                self.metrics["responses_sent"] += 1
                logger.info(f"✅ Single request completed for {strategy_id}: {symbol}.{timeframe}")
            else:
                logger.warning(f"⚠️ No data for single request {strategy_id}: {symbol}.{timeframe}")
                
        except Exception as e:
            logger.error(f"Error executing single request for {strategy_id}: {e}")
            self.metrics["errors"] += 1
    
    async def _handle_unsubscription(self, strategy_id: str, symbol: str, timeframe: str, subscription_key: str):
        """Gère une demande de désabonnement"""
        if subscription_key in self.active_subscriptions:
            self.active_subscriptions[subscription_key]["strategies"].discard(strategy_id)
            
            # Si plus de stratégies abonnées, arrêter le fetch
            if not self.active_subscriptions[subscription_key]["strategies"]:
                if subscription_key in self.fetch_tasks:
                    logger.info(f"🛑 Stopping fetch task for {subscription_key}")
                    self.fetch_tasks[subscription_key].cancel()
                    del self.fetch_tasks[subscription_key]
                del self.active_subscriptions[subscription_key]
                
                # Si plus aucun abonnement, proposer l'arrêt du service
                if not self.active_subscriptions:
                    logger.info("🛑 No more active subscriptions, market service can be stopped")
                    # Optionnel : auto-arrêt du service (selon la politique souhaitée)
                    # await self.stop()
                
        logger.info(f"❌ Subscription removed: {strategy_id} -> {subscription_key}")
        logger.info(f"📊 Active subscriptions remaining: {len(self.active_subscriptions)}")
    
    async def _send_historical_data(self, strategy_id: str, symbol: str, timeframe: str, since: str, to: str, historic: int):
        """Envoie les données historiques à une stratégie"""
        try:
            logger.info(f"📊 Fetching historical data for {strategy_id}: {symbol}.{timeframe}")
            
            # Récupérer les données historiques
            historical_data = await self._fetch_candles_data(symbol, timeframe, since, to, historic)
            
            if historical_data:
                # Envoyer les données historiques
                await self._send_candle_response(strategy_id, symbol, timeframe, historical_data)
                logger.info(f"✅ Historical data sent to {strategy_id}: {symbol}.{timeframe}")
            else:
                logger.warning(f"⚠️ No historical data for {strategy_id}: {symbol}.{timeframe}")
                
        except Exception as e:
            logger.error(f"Error sending historical data to {strategy_id}: {e}")
    
    async def _live_subscription_worker(self, symbol: str, timeframe: str, subscription_key: str, since: str, to: str, historic: int = 200):
        """Worker pour abonnements live avec websocket (futur) ou polling optimisé"""
        try:
            logger.info(f"🔄 Started live subscription worker for {subscription_key}")
            print(f"🔄 LIVE WORKER STARTED: {subscription_key}")
            
            # TODO: À terme, utiliser une websocket Kraken ici
            # Pour l'instant, utilisation d'un polling optimisé plus fréquent
            
            while self.is_running and subscription_key in self.active_subscriptions:
                try:
                    logger.info(f"📡 Live fetching for {subscription_key}...")
                    print(f"📡 LIVE FETCHING: {subscription_key}")
                    
                    # Récupérer seulement les nouvelles données (dernière chandelle)
                    # TODO: Optimiser pour récupérer seulement depuis la dernière chandelle
                    candles_data = await self._fetch_latest_candle(symbol, timeframe)
                    
                    if candles_data:
                        # Vérifier si c'est une nouvelle chandelle
                        last_timestamp = self.active_subscriptions[subscription_key]["last_candle_timestamp"]
                        current_timestamp = candles_data.get('timestamp', 0) if isinstance(candles_data, dict) else 0
                        
                        if current_timestamp > last_timestamp:
                            # Nouvelle chandelle détectée
                            self.active_subscriptions[subscription_key]["last_candle_timestamp"] = current_timestamp
                            
                            # Envoyer à toutes les stratégies abonnées
                            strategies = self.active_subscriptions[subscription_key]["strategies"]
                            for strategy_id in strategies:
                                await self._send_candle_response(strategy_id, symbol, timeframe, candles_data)
                            
                            self.metrics["candles_fetched"] += 1
                            logger.info(f"🔥 New live candle sent to {len(strategies)} strategies")
                        else:
                            logger.debug(f"📊 No new candle for {subscription_key}")
                    
                    # Attendre moins longtemps en mode live (30 secondes au lieu de 60)
                    await asyncio.sleep(30)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in live subscription worker for {subscription_key}: {e}")
                    self.metrics["errors"] += 1
                    await asyncio.sleep(10)  # Pause avant retry
            
        except Exception as e:
            logger.error(f"Fatal error in live subscription worker for {subscription_key}: {e}")
        finally:
            logger.info(f"🛑 Live subscription worker stopped for {subscription_key}")
    
    async def _fetch_latest_candle(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """Récupère seulement la dernière chandelle (optimisation pour live)"""
        try:
            # Pour l'instant, utiliser la méthode existante avec historic=1
            # TODO: Optimiser avec une méthode spécialisée pour la dernière chandelle
            return await self._fetch_candles_data(symbol, timeframe, "now-1h", "now", 1)
        except Exception as e:
            logger.error(f"Error fetching latest candle for {symbol}.{timeframe}: {e}")
            return None
    
    async def _data_fetch_loop(self, symbol: str, timeframe: str, subscription_key: str, since: str, to:str, historic:int=200):
        """Boucle de récupération des données pour un symbole/timeframe"""
        try:
            logger.info(f"🔄 Started data fetch loop for {subscription_key}")
            print(f"🔄 FETCH LOOP STARTED: {subscription_key}")  # Print pour s'assurer de la visibilité
            print(f"🔧 Parameters: symbol={symbol}, timeframe={timeframe}, since={since}, to={to}, historic={historic}")
            print(f"🔧 self.candles_fetcher={self.candles_fetcher}")
            print(f"🔧 self.is_running={self.is_running}")
            print(f"🔧 subscription_key in active_subscriptions: {subscription_key in self.active_subscriptions}")
            
            while self.is_running and subscription_key in self.active_subscriptions:
                try:
                    logger.info(f"🔄 Fetching data for {subscription_key}...")
                    print(f"🔄 FETCHING: {subscription_key}")  # Debug print
                    
                    # Récupérer les données de Kraken
                    candles_data = await self._fetch_candles_data(symbol, timeframe, since, to, historic)
                    
                    if candles_data:
                        # Mettre à jour le DataFrame
                        #await self._update_dataframe(symbol, timeframe, candles_data)
                        
                        # Envoyer à toutes les stratégies abonnées
                        strategies = self.active_subscriptions[subscription_key]["strategies"]
                        for strategy_id in strategies:
                            await self._send_candle_response(strategy_id, symbol, timeframe, candles_data)
                        
                        self.metrics["candles_fetched"] += 1
                    
                    # Attendre avant le prochain fetch (60 secondes)
                    await asyncio.sleep(60)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in data fetch loop for {subscription_key}: {e}")
                    self.metrics["errors"] += 1
                    await asyncio.sleep(5)  # Courte pause avant retry
            
        except Exception as e:
            logger.error(f"Fatal error in data fetch loop for {subscription_key}: {e}")
        finally:
            logger.info(f"🛑 Data fetch loop stopped for {subscription_key}")

    async def _fetch_candles_data(self, symbol: str, timeframe: str, since:str='M', to:str='now', historic:int=200) -> Optional[Dict]:
        """Récupère une chandelle depuis l'API Kraken"""
        try:
            logger.info(f"🔄 Starting CandlesFetcher.get_candles_range with: pair={symbol}, interval={timeframe}, start_date={since}, end_date={to}, historic={historic}")
            
            # Utiliser le CandlesFetcher avec les exchanges configurés
            # TODO: Déterminer l'exchange basé sur le symbol (pour l'instant par défaut)
            exchange = 'kraken_future_spot'  # Par défaut
            
            result = await self.candles_fetcher.get_candles_range(
                exchange=exchange,
                pair=symbol,
                interval=timeframe,
                start_date=since,
                end_date=to,
                historic=historic
            )
            
            logger.info(f"🔍 CandlesFetcher completed. Result type: {type(result)}")
            
            if result is None:
                logger.warning(f"CandlesFetcher returned None for {symbol}.{timeframe}")
                return None
                
            logger.info(f"🔍 CandlesFetcher result: {result}")
                
            return result
                
        except Exception as e:
            logger.error(f"💥 Error fetching candle data for {symbol}.{timeframe}: {e}")
            import traceback
            logger.error(f"💥 Traceback: {traceback.format_exc()}")
            return None
    
    async def _update_dataframe(self, symbol: str, timeframe: str, candle_data: Dict):
        """Met à jour le DataFrame pour ce symbole/timeframe"""
        key = f"{symbol}.{timeframe}"
        
        # Créer le DataFrame s'il n'existe pas
        if key not in self.dataframes:
            self.dataframes[key] = pd.DataFrame()
        
        # Ajouter la nouvelle chandelle
        new_row = pd.DataFrame([candle_data])
        
        # Convertir timestamp en datetime pour l'index
        new_row['datetime'] = pd.to_datetime(new_row['timestamp'], unit='s')
        new_row.set_index('datetime', inplace=True)
        
        # Ajouter au DataFrame principal
        if len(self.dataframes[key]) == 0:
            self.dataframes[key] = new_row
        else:
            # Éviter les doublons basés sur timestamp
            if candle_data['timestamp'] not in self.dataframes[key]['timestamp'].values:
                self.dataframes[key] = pd.concat([self.dataframes[key], new_row])
        
        # Garder seulement les 1000 dernières chandelles
        if len(self.dataframes[key]) > 1000:
            self.dataframes[key] = self.dataframes[key].tail(1000)
        
        logger.debug(f"DataFrame updated for {key}: {len(self.dataframes[key])} rows")
    
    async def _send_candle_response(self, strategy_id: str, symbol: str, timeframe: str, candle_data: Dict):
        """Envoie une réponse avec chandelle et DataFrame à une stratégie"""
        try:
            # Vérification que candle_data est valide
            if not candle_data or not isinstance(candle_data, dict):
                logger.warning(f"Invalid candle_data for {strategy_id}: {candle_data}")
                return
                
            if 'candles' not in candle_data or not candle_data['candles']:
                logger.warning(f"No candles in candle_data for {strategy_id}: {candle_data.keys() if candle_data else 'None'}")
                return
            
            # Canal de réponse
            response_channel = f"market.data.response.{symbol}.{timeframe}.{strategy_id}"
            
            # Préparer le DataFrame à envoyer
            key = f"{symbol}.{timeframe}"
            dataframe_to_send = None
            
            #if key in self.dataframes and len(self.dataframes[key]) > 0:
                # Convertir le DataFrame en dictionnaire pour la sérialisation
            #    dataframe_to_send = self.dataframes[key].to_dict('records')
            
            # Message de réponse
            response_data = {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "candle": candle_data['candles'],
                "candles": candle_data['candles'],
                "dataframe": candle_data.get('historic', []),
                "timestamp": datetime.now(timezone.utc).timestamp(),
                "service": "market_data_service"
            }
            
            # Publier la réponse
            await self.pubsub.publish(response_channel, response_data)
            
            self.metrics["responses_sent"] += 1
            logger.debug(f"📤 Sent candle to {strategy_id}: {symbol} ${candle_data['close']:.2f}")
            
        except Exception as e:
            logger.error(f"Error sending candle response to {strategy_id}: {e}")
            self.metrics["errors"] += 1
    
    async def _send_current_candle(self, strategy_id: str, symbol: str, timeframe: str):
        """Envoie immédiatement une chandelle courante si disponible"""
        try:
            # Récupérer une chandelle immédiatement
            candle_data = await self._fetch_candle_data(symbol, timeframe)
            
            if candle_data:
                await self._update_dataframe(symbol, timeframe, candle_data)
                await self._send_candle_response(strategy_id, symbol, timeframe, candle_data)
                logger.info(f"🚀 Sent initial candle to {strategy_id}: {symbol}")
            
        except Exception as e:
            logger.error(f"Error sending initial candle to {strategy_id}: {e}")
    
    def get_metrics(self) -> Dict:
        """Retourne les métriques du service"""
        return {
            **self.metrics,
            "active_subscriptions": len(self.active_subscriptions),
            "active_fetch_tasks": len(self.fetch_tasks),
            "dataframes_stored": len(self.dataframes)
        }


# Service singleton
_market_service_instance = None

async def get_market_service(pubsub: PubSubEngine, connector: Connector) -> MarketService:
    """Factory pour obtenir l'instance du service Market"""
    global _market_service_instance
    
    if _market_service_instance is None:
        _market_service_instance = MarketService(pubsub, connector)
        await _market_service_instance.start()
    
    return _market_service_instance