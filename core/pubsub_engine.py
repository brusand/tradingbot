import asyncio
import logging
from typing import Dict, List, Callable, Any, Optional
from datetime import datetime, UTC
import json
import weakref
import fnmatch

import click


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
        self.logger.setLevel(logging.DEBUG)
    
    async def start(self):
        """Démarre le moteur PubSub"""
        if self.is_running:
            return
        
        self.is_running = True
        self.processor_task = asyncio.create_task(self._message_processor())
        self.logger.info("PubSub engine started")
        self.logger.info("🔥 Message processor task created")
    
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
            click.echo(f"🔍 [PUBSUB][PUBLISH] {channel}({message})", err=True)

        except asyncio.TimeoutError:
            self.logger.warning(f"Message queue full, dropping message for channel '{channel}'")
        except Exception as e:
            self.logger.error(f"Error publishing message to '{channel}': {e}")
    
    async def _message_processor(self):
        """Traite les messages de manière asynchrone"""
        self.logger.info("🔥 Message processor started!")
        while self.is_running:
            try:
                # Attendre un message avec timeout
                self.logger.debug("Waiting for message...")
                message_data = await asyncio.wait_for(
                    self.message_queue.get(),
                    timeout=1.0
                )
                print(f"🔥 Got message: {message_data}")
                self.logger.info(f"🔥 Got message: {message_data}")
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
        
        # Trouver tous les patterns qui matchent ce channel
        matching_patterns = []
        self.logger.debug(f"Looking for patterns matching channel '{channel}'")
        self.logger.debug(f"Available patterns: {list(self.subscribers.keys())}")
        
        for pattern in self.subscribers.keys():
            exact_match = pattern == channel
            wildcard_match = fnmatch.fnmatch(channel, pattern)
            self.logger.debug(f"Pattern '{pattern}': exact={exact_match}, wildcard={wildcard_match}")
            
            if exact_match or wildcard_match:
                matching_patterns.append(pattern)
        
        self.logger.debug(f"Matching patterns for '{channel}': {matching_patterns}")
        
        if not matching_patterns:
            self.logger.warning(f"No matching patterns found for channel '{channel}'")
            return
        
        # Collecter tous les abonnés pour tous les patterns qui matchent
        all_active_subscribers = []
        
        for pattern in matching_patterns:
            subscribers_list = self.subscribers[pattern]
            pattern_subscribers = []
            self.logger.debug(f"Processing pattern '{pattern}' with {len(subscribers_list)} subscribers")
            
            for i, subscriber in enumerate(subscribers_list):
                # Gérer les weak references
                if hasattr(subscriber, '__call__'):
                    # Callback normal
                    callback = subscriber
                    self.logger.debug(f"  Subscriber {i}: direct callback {callback}")
                else:
                    # WeakMethod ou WeakRef
                    callback = subscriber()
                    if callback is None:
                        # Weak reference morte, ignorer
                        self.stats["subscribers_count"] -= 1
                        self.logger.debug(f"  Subscriber {i}: dead weak reference, skipping")
                        continue
                    self.logger.debug(f"  Subscriber {i}: weak ref callback {callback}")
                
                pattern_subscribers.append(callback)
                all_active_subscribers.append(callback)
            
            # TODO: Fix cette logique de mise à jour qui efface les abonnés
            # Mettre à jour la liste avec seulement les abonnés actifs pour ce pattern
            # active_subs_for_pattern = []
            # for sub in subscribers_list:
            #     if hasattr(sub, '__call__'):
            #         if sub in pattern_subscribers:
            #             active_subs_for_pattern.append(sub)
            #     else:
            #         callback_ref = sub()
            #         if callback_ref is not None and callback_ref in pattern_subscribers:
            #             active_subs_for_pattern.append(sub)
            # 
            # self.subscribers[pattern] = active_subs_for_pattern
        
        # Livrer le message à tous les abonnés actifs (sans doublons)
        unique_subscribers = list(set(all_active_subscribers))
        self.logger.debug(f"Total unique subscribers to call: {len(unique_subscribers)}")
        
        # Exécuter directement les callbacks au lieu de créer des tâches
        if unique_subscribers:
            results = []
            for i, callback in enumerate(unique_subscribers):
                self.logger.debug(f"Executing callback {i} directly: {callback}")
                try:
                    # Exécution directe avec timeout
                    result = await asyncio.wait_for(
                        self._safe_callback_call(callback, message, metadata),
                        timeout=15.0
                    )
                    results.append(result)
                    self.logger.debug(f"✅ Callback {i} completed successfully")
                except asyncio.TimeoutError:
                    self.logger.error(f"❌ Timeout in callback {i}: {callback}")
                    results.append(Exception("Timeout"))
                except Exception as e:
                    self.logger.error(f"❌ Error in callback {i}: {e}")
                    results.append(e)
                
                # Petit délai pour permettre aux logs de s'afficher
                await asyncio.sleep(0.01)
            
            self.logger.debug(f"Gather completed, results: {results}")
            
            # Compter les livraisons réussies
            successful_deliveries = sum(1 for result in results if not isinstance(result, Exception))
            failed_deliveries = len(results) - successful_deliveries
            
            self.stats["messages_delivered"] += successful_deliveries
            
            self.logger.debug(f"Delivered message to {successful_deliveries}/{len(unique_subscribers)} subscribers on '{channel}' (patterns: {matching_patterns})")
            if failed_deliveries > 0:
                self.logger.warning(f"{failed_deliveries} delivery failures on '{channel}'")
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.logger.error(f"Task {i} failed with: {result}")
        else:
            self.logger.warning(f"No subscribers found for channel '{channel}'")
    
    async def _safe_callback_call(self, callback: Callable, message: Any, metadata: Dict):
        """Appelle un callback de manière sécurisée"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(message, metadata)
            else:
                callback(message, metadata)
        except Exception as e:
            self.logger.error(f"🚨 Error in subscriber callback {callback}: {e}")
            import traceback
            self.logger.error(f"🚨 Traceback: {traceback.format_exc()}")
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