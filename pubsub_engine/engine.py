"""
PubSub Engine principal - Port Python du PubSubEngine TypeScript
Utilise Redis et WebSocket pour la communication inter-services
"""

import asyncio
import json
import logging
from typing import Dict, Set, Any, Optional, Callable, List
import aioredis
import websockets
from websockets.server import WebSocketServerProtocol
import uuid
import time

from .types import (
    PubSubConfig, 
    SubscriptionPattern, 
    Subscription,
    PubMessage,
    SubMetadata,
    PubMetadata,
    ChannelConfig,
    Channel,
    SubscriptionInfo,
    generate_id,
    current_timestamp
)
from .constants import DEFAULT_PUB_OPTIONS


logger = logging.getLogger(__name__)


class PubSubEngine:
    """
    Engine PubSub principal compatible avec la version TypeScript
    Gère les publications/souscriptions via Redis et WebSocket
    """
    
    def __init__(self, config: PubSubConfig):
        self.config = config
        self.redis_client: Optional[aioredis.Redis] = None
        self.redis_subscriber: Optional[aioredis.Redis] = None
        self.ws_server = None
        self.ws_clients: Set[WebSocketServerProtocol] = set()
        
        # Stockage des souscriptions
        self.subscriptions: Dict[str, Set[Subscription]] = {}
        self.channel_patterns: Dict[str, str] = {}
        
        # Métriques
        self.metrics = {
            "messages_published": 0,
            "messages_delivered": 0,
            "active_subscriptions": 0,
            "last_message_timestamp": 0
        }
        
        # État
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        """Démarre le PubSub Engine"""
        try:
            logger.info("Starting PubSub Engine...")
            
            # Initialiser Redis
            await self._init_redis()
            
            # Démarrer WebSocket server si configuré
            if self.config.websocket:
                await self._init_websocket()
            
            # Démarrer la tâche d'écoute Redis
            task = asyncio.create_task(self._redis_listener())
            self._tasks.append(task)
            
            self._running = True
            logger.info("PubSub Engine started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start PubSub Engine: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Arrête le PubSub Engine"""
        logger.info("Stopping PubSub Engine...")
        
        self._running = False
        
        # Arrêter les tâches
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Fermer WebSocket server
        if self.ws_server:
            self.ws_server.close()
            await self.ws_server.wait_closed()
        
        # Fermer Redis
        if self.redis_client:
            await self.redis_client.close()
        if self.redis_subscriber:
            await self.redis_subscriber.close()
            
        logger.info("PubSub Engine stopped")

    async def _init_redis(self) -> None:
        """Initialise les connexions Redis"""
        redis_config = self.config.redis
        
        # Préparer les paramètres de connexion
        connection_params = {
            "host": redis_config.host,
            "port": redis_config.port,
            "db": redis_config.db
        }
        
        if redis_config.password:
            connection_params["password"] = redis_config.password
        if redis_config.username:
            connection_params["username"] = redis_config.username
        
        # Client principal pour publish
        self.redis_client = aioredis.Redis(**connection_params)
        
        # Client séparé pour les souscriptions
        self.redis_subscriber = aioredis.Redis(**connection_params)
        
        # Test de connexion
        await self.redis_client.ping()
        await self.redis_subscriber.ping()
        
        logger.info("Redis connections established")

    async def _init_websocket(self) -> None:
        """Initialise le serveur WebSocket"""
        ws_config = self.config.websocket
        
        async def handle_client(websocket: WebSocketServerProtocol, path: str):
            self.ws_clients.add(websocket)
            logger.info(f"WebSocket client connected: {websocket.remote_address}")
            
            try:
                async for message in websocket:
                    await self._handle_ws_message(websocket, message)
            except websockets.exceptions.ConnectionClosed:
                pass
            finally:
                self.ws_clients.discard(websocket)
                logger.info(f"WebSocket client disconnected: {websocket.remote_address}")
        
        self.ws_server = await websockets.serve(
            handle_client,
            ws_config.host,
            ws_config.port
        )
        
        logger.info(f"WebSocket server started on {ws_config.host}:{ws_config.port}")

    async def _handle_ws_message(self, websocket: WebSocketServerProtocol, message: str) -> None:
        """Gère les messages WebSocket entrants"""
        try:
            data = json.loads(message)
            
            if data.get("type") == "subscribe" and data.get("pattern"):
                # Créer une souscription WebSocket
                pattern = SubscriptionPattern(**data["pattern"])
                
                def ws_callback(msg_data: Dict[str, Any], metadata: SubMetadata):
                    asyncio.create_task(self._send_to_websocket(websocket, {
                        "type": "message",
                        "channel": metadata.channel,
                        "data": msg_data,
                        "metadata": {
                            "timestamp": metadata.timestamp,
                            "message_id": metadata.message_id,
                            "latency": metadata.latency
                        }
                    }))
                
                subscription = await self.subscribe(pattern, ws_callback)
                
                await self._send_to_websocket(websocket, {
                    "type": "subscribed",
                    "id": subscription.id,
                    "pattern": data["pattern"]
                })
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
            await self._send_to_websocket(websocket, {
                "type": "error",
                "message": str(e)
            })

    async def _send_to_websocket(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Envoie un message via WebSocket"""
        try:
            await websocket.send(json.dumps(data))
        except Exception as e:
            logger.error(f"Error sending WebSocket message: {e}")

    async def _redis_listener(self) -> None:
        """Écoute les messages Redis en arrière-plan"""
        pubsub = self.redis_subscriber.pubsub()
        
        try:
            while self._running:
                # S'abonner aux canaux actifs
                active_channels = list(self.subscriptions.keys())
                if active_channels:
                    await pubsub.subscribe(*active_channels)
                
                # Écouter les messages
                try:
                    message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=1.0)
                    if message and message['type'] == 'message':
                        await self._handle_redis_message(message['channel'].decode(), message['data'].decode())
                except asyncio.TimeoutError:
                    continue
                    
        except Exception as e:
            logger.error(f"Redis listener error: {e}")
        finally:
            await pubsub.close()

    async def _handle_redis_message(self, channel: str, message_data: str) -> None:
        """Traite un message Redis reçu"""
        try:
            parsed_message = json.loads(message_data)
            now = current_timestamp()
            
            # Trouver les souscriptions correspondantes
            if channel in self.subscriptions:
                for subscription in self.subscriptions[channel].copy():
                    if not subscription.is_active:
                        continue
                    
                    # Vérifier les filtres
                    if self._should_deliver_message(subscription, parsed_message):
                        metadata = SubMetadata(
                            channel=channel,
                            timestamp=parsed_message.get("timestamp", now),
                            message_id=parsed_message.get("message_id", generate_id()),
                            receive_timestamp=now,
                            latency=now - parsed_message.get("timestamp", now)
                        )
                        
                        # Appeler le callback
                        try:
                            if asyncio.iscoroutinefunction(subscription.callback):
                                await subscription.callback(parsed_message.get("data", {}), metadata)
                            else:
                                subscription.callback(parsed_message.get("data", {}), metadata)
                            
                            self.metrics["messages_delivered"] += 1
                        except Exception as e:
                            logger.error(f"Error in subscription callback for {channel}: {e}")
                            
        except Exception as e:
            logger.error(f"Error handling Redis message on channel {channel}: {e}")

    def _should_deliver_message(self, subscription: Subscription, message: Dict[str, Any]) -> bool:
        """Vérifie si un message doit être livré à une souscription"""
        pattern = subscription.pattern
        data = message.get("data", {})
        
        # Filtre par symbole
        if pattern.symbol and pattern.symbol != "*" and data.get("symbol") != pattern.symbol:
            return False
        
        # Filtre par timeframe
        if pattern.timeframe and pattern.timeframe != "*" and data.get("timeframe") != pattern.timeframe:
            return False
        
        # Filtres personnalisés
        if pattern.filters:
            for key, filter_value in pattern.filters.items():
                data_value = data.get(key)
                if data_value is None:
                    return False
                
                if isinstance(filter_value, list):
                    if data_value not in filter_value:
                        return False
                elif data_value != filter_value:
                    return False
        
        return True

    def _pattern_to_redis_channel(self, pattern: SubscriptionPattern) -> str:
        """Convertit un pattern en canal Redis"""
        service = pattern.service
        symbol = pattern.symbol or "*"
        timeframe = pattern.timeframe or "*"
        data_type = pattern.data_type
        
        if isinstance(data_type, list):
            # Types multiples
            return f"{service}:{symbol}:{timeframe}:*"
        
        return f"{service}:{symbol}:{timeframe}:{data_type}"

    def _resolve_channel_pattern(self, channel: str, data: Dict[str, Any]) -> str:
        """Résout les variables dans un pattern de canal"""
        resolved = channel
        
        # Remplacer les variables par les valeurs des données
        replacements = {
            "{symbol}": data.get("symbol", "*"),
            "{timeframe}": data.get("timeframe", "*"),
            "{indicator}": data.get("indicator", "*"),
            "{strategyId}": data.get("strategyId", "*"),
            "{orderId}": data.get("orderId", "*"),
            "{userId}": data.get("userId", "*"),
            "{severity}": data.get("severity", "*"),
            "{type}": data.get("type", "*")
        }
        
        for placeholder, value in replacements.items():
            resolved = resolved.replace(placeholder, str(value))
        
        return resolved

    async def publish(
        self, 
        channel: str, 
        data: Dict[str, Any], 
        metadata: Optional[PubMetadata] = None
    ) -> None:
        """Publie un message sur un canal"""
        if not self._running:
            raise RuntimeError("PubSub Engine is not running")
        
        now = current_timestamp()
        resolved_channel = self._resolve_channel_pattern(channel, data)
        
        message = PubMessage(
            channel=resolved_channel,
            data=data,
            timestamp=now,
            message_id=generate_id(),
            metadata=metadata or DEFAULT_PUB_OPTIONS
        )
        
        message_json = message.model_dump_json()
        
        try:
            # Publier sur Redis si persistance activée
            if self.config.persistence or (metadata and metadata.persistent):
                await self.redis_client.publish(resolved_channel, message_json)
                
                # Stocker le message avec TTL si spécifié
                ttl = (metadata.ttl if metadata else None) or self.config.default_ttl
                if ttl:
                    message_key = f"message:{message.message_id}"
                    await self.redis_client.setex(message_key, ttl // 1000, message_json)
            
            # Diffuser aux abonnés locaux
            await self._broadcast_to_subscribers(resolved_channel, message.model_dump())
            
            # Mettre à jour les métriques
            self.metrics["messages_published"] += 1
            self.metrics["last_message_timestamp"] = now
            
        except Exception as e:
            logger.error(f"Error publishing to Redis channel {resolved_channel}: {e}")
            raise

    async def publish_batch(self, messages: List[Dict[str, Any]]) -> None:
        """Publie plusieurs messages en lot"""
        if not messages:
            return
        
        now = current_timestamp()
        
        # Utiliser une pipeline Redis pour l'efficacité
        pipe = self.redis_client.pipeline()
        batch = []
        
        for msg_data in messages:
            channel = msg_data["channel"]
            data = msg_data["data"]
            metadata = msg_data.get("metadata")
            
            resolved_channel = self._resolve_channel_pattern(channel, data)
            
            message = PubMessage(
                channel=resolved_channel,
                data=data,
                timestamp=now,
                message_id=generate_id(),
                metadata=PubMetadata(**metadata) if metadata else DEFAULT_PUB_OPTIONS
            )
            
            batch.append(message)
            
            # Ajouter à la pipeline Redis
            if self.config.persistence or (metadata and metadata.get("persistent")):
                pipe.publish(resolved_channel, message.model_dump_json())
                
                ttl = (metadata.get("ttl") if metadata else None) or self.config.default_ttl
                if ttl:
                    message_key = f"message:{message.message_id}"
                    pipe.setex(message_key, ttl // 1000, message.model_dump_json())
        
        # Exécuter la pipeline Redis
        if pipe._command_stack:
            await pipe.execute()
        
        # Diffuser aux abonnés locaux
        for message in batch:
            await self._broadcast_to_subscribers(message.channel, message.model_dump())
        
        # Mettre à jour les métriques
        self.metrics["messages_published"] += len(batch)
        self.metrics["last_message_timestamp"] = now

    async def _broadcast_to_subscribers(self, channel: str, message: Dict[str, Any]) -> None:
        """Diffuse un message aux abonnés locaux"""
        if channel in self.subscriptions:
            subs = self.subscriptions[channel].copy()
            now = current_timestamp()
            
            for subscription in subs:
                if not subscription.is_active:
                    continue
                
                if self._should_deliver_message(subscription, message):
                    metadata = SubMetadata(
                        channel=channel,
                        timestamp=message.get("timestamp", now),
                        message_id=message.get("message_id", generate_id()),
                        receive_timestamp=now,
                        latency=now - message.get("timestamp", now)
                    )
                    
                    try:
                        if asyncio.iscoroutinefunction(subscription.callback):
                            await subscription.callback(message.get("data", {}), metadata)
                        else:
                            subscription.callback(message.get("data", {}), metadata)
                        
                        self.metrics["messages_delivered"] += 1
                    except Exception as e:
                        logger.error(f"Error in subscription callback for {channel}: {e}")

    async def subscribe(
        self, 
        pattern: SubscriptionPattern, 
        callback: Callable[[Dict[str, Any], SubMetadata], None]
    ) -> Subscription:
        """S'abonne à un pattern de canal"""
        subscription_id = generate_id()
        redis_channel = self._pattern_to_redis_channel(pattern)
        
        subscription = Subscription(
            id=subscription_id,
            pattern=pattern,
            callback=callback,
            created_at=current_timestamp(),
            is_active=True
        )
        
        # Ajouter aux souscriptions
        if redis_channel not in self.subscriptions:
            self.subscriptions[redis_channel] = set()
        
        self.subscriptions[redis_channel].add(subscription)
        
        # Mettre à jour les métriques
        self.metrics["active_subscriptions"] += 1
        
        logger.debug(f"Created subscription {subscription_id} for pattern {pattern}")
        
        return subscription

    async def unsubscribe(self, subscription: Subscription) -> None:
        """Se désabonne d'un canal"""
        redis_channel = self._pattern_to_redis_channel(subscription.pattern)
        
        if redis_channel in self.subscriptions:
            self.subscriptions[redis_channel].discard(subscription)
            
            # Supprimer le canal s'il n'y a plus d'abonnés
            if not self.subscriptions[redis_channel]:
                del self.subscriptions[redis_channel]
            
            # Mettre à jour les métriques
            self.metrics["active_subscriptions"] -= 1
            
            logger.debug(f"Removed subscription {subscription.id}")

    async def create_channel(self, channel_config: ChannelConfig) -> Channel:
        """Crée un nouveau canal"""
        channel_id = generate_id()
        
        # Stocker la configuration du canal
        self.channel_patterns[channel_config.name] = channel_config.pattern
        
        # Stocker dans Redis pour la persistance
        await self.redis_client.set(f"channel:{channel_id}", channel_config.model_dump_json())
        
        return Channel(
            id=channel_id,
            config=channel_config,
            subscription_count=0
        )

    def list_active_subscriptions(self) -> List[SubscriptionInfo]:
        """Liste toutes les souscriptions actives"""
        result = []
        
        for channel, subs in self.subscriptions.items():
            for sub in subs:
                if sub.is_active:
                    result.append(SubscriptionInfo(
                        id=sub.id,
                        pattern=sub.pattern,
                        created_at=sub.created_at,
                        is_active=sub.is_active,
                        message_count=0  # TODO: tracker le nombre de messages
                    ))
        
        return result

    def get_metrics(self) -> Dict[str, Any]:
        """Retourne les métriques actuelles"""
        return {
            **self.metrics,
            "timestamp": current_timestamp()
        }

    async def health_check(self) -> Dict[str, Any]:
        """Vérifie la santé du système"""
        try:
            # Test Redis
            await self.redis_client.ping()
            redis_status = "healthy"
        except Exception as e:
            redis_status = f"unhealthy: {e}"
        
        return {
            "status": "healthy" if redis_status == "healthy" else "unhealthy",
            "redis": redis_status,
            "websocket_clients": len(self.ws_clients),
            "active_subscriptions": self.metrics["active_subscriptions"],
            "uptime": current_timestamp() - self.metrics.get("start_time", current_timestamp()),
            "metrics": self.get_metrics()
        }