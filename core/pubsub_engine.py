import asyncio
import logging
from typing import Dict, List, Callable, Any, Optional
from datetime import datetime, UTC
import json
import weakref


class PubSubEngine:
    """Moteur PubSub pour communication asynchrone entre services"""
    
    def __init__(self):
        # Structure: channel -> liste de callbacks
        self.subscribers: Dict[str, List[Callable]] = {}
        
        # Statistiques
        self.stats = {
            "messages_published": 0,
            "messages_delivered": 0,
            "channels_count": 0,
            "subscribers_count": 0
        }
        
        # File d'attente pour traitement asynchrone
        self.message_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.processor_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        self.logger = logging.getLogger("PubSubEngine")
    
    async def start(self):
        """Démarre le moteur PubSub"""
        if self.is_running:
            return
        
        self.is_running = True
        self.processor_task = asyncio.create_task(self._message_processor())
        self.logger.info("PubSub engine started")
    
    async def stop(self):
        """Arrête le moteur PubSub"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.processor_task:
            self.processor_task.cancel()
            try:
                await self.processor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("PubSub engine stopped")
    
    async def subscribe(self, channel: str, callback: Callable, weak_ref: bool = True):
        """
        S'abonne à un canal
        
        Args:
            channel: Nom du canal
            callback: Fonction à appeler lors de réception de message
            weak_ref: Si True, utilise une weak reference (recommandé)
        """
        if channel not in self.subscribers:
            self.subscribers[channel] = []
            self.stats["channels_count"] += 1
        
        # Utiliser weak reference pour éviter les fuites mémoire
        if weak_ref and hasattr(callback, '__self__'):
            # C'est une méthode d'instance, utiliser WeakMethod
            callback_ref = weakref.WeakMethod(callback)
        else:
            # Fonction normale ou weak_ref=False
            callback_ref = callback
        
        self.subscribers[channel].append(callback_ref)
        self.stats["subscribers_count"] += 1
        
        self.logger.debug(f"Subscribed to channel '{channel}', total subscribers: {len(self.subscribers[channel])}")
    
    async def unsubscribe(self, channel: str, callback: Callable):
        """Se désabonne d'un canal"""
        if channel not in self.subscribers:
            return
        
        # Trouver et supprimer le callback
        subscribers_list = self.subscribers[channel]
        to_remove = []
        
        for i, sub in enumerate(subscribers_list):
            # Gérer les weak references
            if hasattr(sub, '__call__'):
                actual_callback = sub
            else:
                actual_callback = sub()  # WeakMethod
                if actual_callback is None:
                    to_remove.append(i)
                    continue
            
            if actual_callback == callback:
                to_remove.append(i)
        
        # Supprimer en ordre inverse pour préserver les indices
        for i in reversed(to_remove):
            del subscribers_list[i]
            self.stats["subscribers_count"] -= 1
        
        # Nettoyer le canal s'il est vide
        if not subscribers_list:
            del self.subscribers[channel]
            self.stats["channels_count"] -= 1
        
        self.logger.debug(f"Unsubscribed from channel '{channel}'")
    
    async def publish(self, channel: str, message: Any, metadata: Optional[Dict] = None):
        """
        Publie un message sur un canal
        
        Args:
            channel: Nom du canal
            message: Message à publier
            metadata: Métadonnées optionnelles
        """
        try:
            message_data = {
                "channel": channel,
                "message": message,
                "metadata": metadata or {},
                "timestamp": datetime.now(UTC),
                "message_id": f"{channel}_{self.stats['messages_published']}"
            }
            
            # Ajouter à la queue pour traitement asynchrone
            await asyncio.wait_for(
                self.message_queue.put(message_data),
                timeout=1.0
            )
            
            self.stats["messages_published"] += 1
            
        except asyncio.TimeoutError:
            self.logger.warning(f"Message queue full, dropping message for channel '{channel}'")
        except Exception as e:
            self.logger.error(f"Error publishing message to '{channel}': {e}")
    
    async def _message_processor(self):
        """Traite les messages de manière asynchrone"""
        while self.is_running:
            try:
                # Attendre un message avec timeout
                message_data = await asyncio.wait_for(
                    self.message_queue.get(),
                    timeout=1.0
                )
                
                await self._deliver_message(message_data)
                self.message_queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout normal, continuer
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in message processor: {e}")
                await asyncio.sleep(0.1)
    
    async def _deliver_message(self, message_data: Dict):
        """Livre un message à tous les abonnés du canal"""
        channel = message_data["channel"]
        message = message_data["message"]
        metadata = message_data["metadata"]
        
        if channel not in self.subscribers:
            return
        
        # Nettoyer les weak references mortes
        subscribers_list = self.subscribers[channel]
        active_subscribers = []
        
        for subscriber in subscribers_list:
            # Gérer les weak references
            if hasattr(subscriber, '__call__'):
                # Callback normal
                callback = subscriber
            else:
                # WeakMethod ou WeakRef
                callback = subscriber()
                if callback is None:
                    # Weak reference morte, ignorer
                    self.stats["subscribers_count"] -= 1
                    continue
            
            active_subscribers.append(callback)
        
        # Mettre à jour la liste avec seulement les abonnés actifs
        self.subscribers[channel] = [sub for sub in subscribers_list if sub in [cb for cb in active_subscribers]]
        
        # Livrer le message à tous les abonnés actifs
        delivery_tasks = []
        for callback in active_subscribers:
            task = asyncio.create_task(self._safe_callback_call(callback, message, metadata))
            delivery_tasks.append(task)
        
        if delivery_tasks:
            # Attendre que tous les callbacks soient exécutés
            results = await asyncio.gather(*delivery_tasks, return_exceptions=True)
            
            # Compter les livraisons réussies
            successful_deliveries = sum(1 for result in results if not isinstance(result, Exception))
            self.stats["messages_delivered"] += successful_deliveries
            
            self.logger.debug(f"Delivered message to {successful_deliveries}/{len(delivery_tasks)} subscribers on '{channel}'")
    
    async def _safe_callback_call(self, callback: Callable, message: Any, metadata: Dict):
        """Appelle un callback de manière sécurisée"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(message, metadata)
            else:
                callback(message, metadata)
        except Exception as e:
            self.logger.error(f"Error in subscriber callback: {e}")
            raise
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques du moteur PubSub"""
        return {
            **self.stats,
            "queue_size": self.message_queue.qsize(),
            "is_running": self.is_running,
            "channels": list(self.subscribers.keys())
        }
    
    def get_channel_info(self, channel: str) -> Dict:
        """Retourne les informations sur un canal spécifique"""
        if channel not in self.subscribers:
            return {"exists": False}
        
        subscribers_count = len(self.subscribers[channel])
        return {
            "exists": True,
            "subscribers_count": subscribers_count,
            "channel": channel
        }
    
    async def publish_sync(self, channel: str, message: Any, metadata: Optional[Dict] = None):
        """
        Publie un message de manière synchrone (attend la livraison)
        Attention: peut être plus lent, à utiliser avec parcimonie
        """
        if channel not in self.subscribers:
            return
        
        message_data = {
            "channel": channel,
            "message": message,
            "metadata": metadata or {},
            "timestamp": datetime.now(UTC),
            "message_id": f"{channel}_{self.stats['messages_published']}_sync"
        }
        
        await self._deliver_message(message_data)
        self.stats["messages_published"] += 1
    
    def cleanup_dead_references(self):
        """Nettoie les weak references mortes"""
        total_cleaned = 0
        channels_to_remove = []
        
        for channel, subscribers_list in self.subscribers.items():
            active_subscribers = []
            
            for subscriber in subscribers_list:
                if hasattr(subscriber, '__call__'):
                    # Callback normal
                    active_subscribers.append(subscriber)
                else:
                    # Weak reference
                    callback = subscriber()
                    if callback is not None:
                        active_subscribers.append(subscriber)
                    else:
                        total_cleaned += 1
                        self.stats["subscribers_count"] -= 1
            
            if active_subscribers:
                self.subscribers[channel] = active_subscribers
            else:
                channels_to_remove.append(channel)
        
        # Supprimer les canaux vides
        for channel in channels_to_remove:
            del self.subscribers[channel]
            self.stats["channels_count"] -= 1
        
        if total_cleaned > 0:
            self.logger.info(f"Cleaned up {total_cleaned} dead references, removed {len(channels_to_remove)} empty channels")


# Instance globale pour faciliter l'utilisation
global_pubsub = PubSubEngine()