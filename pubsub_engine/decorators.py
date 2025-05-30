"""
Décorateurs pour le PubSub Engine
Compatible avec les décorateurs TypeScript existants
"""

import asyncio
import functools
import logging
from typing import Any, Callable, Optional, Dict
import weakref

from .types import SubscriptionPattern, Subscription, generate_id, current_timestamp
from .constants import DEFAULT_PUB_OPTIONS


logger = logging.getLogger(__name__)


def Subscribe(pattern: SubscriptionPattern):
    """
    Décorateur pour s'abonner automatiquement à un pattern de canal
    Compatible avec le décorateur TypeScript Subscribe
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            # Initialiser les souscriptions si nécessaire
            if not hasattr(self, '_subscriptions'):
                self._subscriptions = {}
            
            # Générer une clé pour ce pattern de souscription
            pattern_key = f"{pattern.service}:{pattern.data_type}:{pattern.symbol}:{pattern.timeframe}"
            
            # S'abonner si pas déjà fait
            if pattern_key not in self._subscriptions:
                # S'assurer que l'instance a un pubsub
                if not hasattr(self, 'pubsub'):
                    raise AttributeError('PubSub instance not found. Make sure this class has a pubsub property.')
                
                # Créer le callback qui appelle la méthode originale
                def callback(data: Dict[str, Any], metadata):
                    if asyncio.iscoroutinefunction(func):
                        asyncio.create_task(func(self, data, metadata, *args, **kwargs))
                    else:
                        func(self, data, metadata, *args, **kwargs)
                
                subscription = await self.pubsub.subscribe(pattern, callback)
                self._subscriptions[pattern_key] = subscription
                
                logger.debug(f"Auto-subscribed to pattern: {pattern_key}")
            
            # Appeler la méthode originale
            result = await func(self, *args, **kwargs)
            return result
        
        def sync_wrapper(self, *args, **kwargs):
            # Version synchrone pour compatibilité
            if not hasattr(self, '_subscriptions'):
                self._subscriptions = {}
            
            pattern_key = f"{pattern.service}:{pattern.data_type}:{pattern.symbol}:{pattern.timeframe}"
            
            if pattern_key not in self._subscriptions:
                if not hasattr(self, 'pubsub'):
                    raise AttributeError('PubSub instance not found. Make sure this class has a pubsub property.')
                
                def callback(data: Dict[str, Any], metadata):
                    func(self, data, metadata, *args, **kwargs)
                
                # Pour les méthodes sync, on crée une tâche async pour la souscription
                async def subscribe_async():
                    subscription = await self.pubsub.subscribe(pattern, callback)
                    self._subscriptions[pattern_key] = subscription
                
                if hasattr(self.pubsub, '_loop') and self.pubsub._loop.is_running():
                    asyncio.create_task(subscribe_async())
                
                logger.debug(f"Auto-subscribed to pattern (sync): {pattern_key}")
            
            return func(self, *args, **kwargs)
        
        # Retourner le wrapper approprié selon si la fonction est async ou sync
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def Publish(channel: str, get_data_fn: Optional[Callable] = None):
    """
    Décorateur pour publier automatiquement un message quand une méthode est appelée
    Compatible avec le décorateur TypeScript Publish
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            # Appeler la méthode originale
            result = await func(self, *args, **kwargs)
            
            # Publier via PubSub
            if hasattr(self, 'pubsub'):
                try:
                    # Préparer les données à publier
                    if get_data_fn:
                        data = get_data_fn(result, *args, **kwargs)
                    else:
                        data = result if isinstance(result, dict) else {"result": result}
                    
                    # Publier le message
                    await self.pubsub.publish(channel, data)
                    
                    logger.debug(f"Auto-published to channel: {channel}")
                    
                except Exception as e:
                    logger.error(f"Error auto-publishing to {channel}: {e}")
            
            return result
        
        def sync_wrapper(self, *args, **kwargs):
            # Appeler la méthode originale
            result = func(self, *args, **kwargs)
            
            # Publier via PubSub (version async dans une tâche)
            if hasattr(self, 'pubsub'):
                try:
                    if get_data_fn:
                        data = get_data_fn(result, *args, **kwargs)
                    else:
                        data = result if isinstance(result, dict) else {"result": result}
                    
                    # Créer une tâche async pour la publication
                    async def publish_async():
                        await self.pubsub.publish(channel, data)
                    
                    if hasattr(self.pubsub, '_loop') and self.pubsub._loop.is_running():
                        asyncio.create_task(publish_async())
                    
                    logger.debug(f"Auto-published to channel (sync): {channel}")
                    
                except Exception as e:
                    logger.error(f"Error auto-publishing to {channel}: {e}")
            
            return result
        
        # Retourner le wrapper approprié
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def AutoUnsubscribe(cls):
    """
    Décorateur de classe pour nettoyer automatiquement les souscriptions
    Compatible avec le décorateur TypeScript AutoUnsubscribe
    """
    original_init = cls.__init__
    
    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        # Initialiser le dictionnaire des souscriptions
        if not hasattr(self, '_subscriptions'):
            self._subscriptions = {}
    
    # Sauvegarder la méthode destroy originale si elle existe
    original_destroy = getattr(cls, 'destroy', None)
    
    async def destroy(self):
        """Nettoie toutes les souscriptions"""
        if hasattr(self, '_subscriptions') and hasattr(self, 'pubsub'):
            for subscription in self._subscriptions.values():
                try:
                    await self.pubsub.unsubscribe(subscription)
                except Exception as e:
                    logger.error(f"Error unsubscribing: {e}")
            
            self._subscriptions.clear()
            logger.debug(f"Cleaned up subscriptions for {cls.__name__}")
        
        # Appeler la méthode destroy originale si elle existe
        if original_destroy:
            if asyncio.iscoroutinefunction(original_destroy):
                await original_destroy(self)
            else:
                original_destroy(self)
    
    # Remplacer les méthodes
    cls.__init__ = new_init
    cls.destroy = destroy
    
    return cls


class PublishResult:
    """
    Classe helper pour les résultats de publication
    Permet de spécifier des métadonnées de publication
    """
    
    def __init__(self, data: Any, metadata: Optional[Dict[str, Any]] = None):
        self.data = data
        self.metadata = metadata or {}


def publish_on_success(channel: str, extract_data: Optional[Callable] = None):
    """
    Décorateur qui publie uniquement en cas de succès
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            try:
                result = await func(self, *args, **kwargs)
                
                # Publier seulement si succès
                if hasattr(self, 'pubsub'):
                    data = extract_data(result, *args, **kwargs) if extract_data else result
                    await self.pubsub.publish(channel, data)
                
                return result
            except Exception as e:
                # Ne pas publier en cas d'erreur
                logger.debug(f"Not publishing due to error in {func.__name__}: {e}")
                raise
        
        def sync_wrapper(self, *args, **kwargs):
            try:
                result = func(self, *args, **kwargs)
                
                if hasattr(self, 'pubsub'):
                    data = extract_data(result, *args, **kwargs) if extract_data else result
                    
                    async def publish_async():
                        await self.pubsub.publish(channel, data)
                    
                    if hasattr(self.pubsub, '_loop') and self.pubsub._loop.is_running():
                        asyncio.create_task(publish_async())
                
                return result
            except Exception as e:
                logger.debug(f"Not publishing due to error in {func.__name__}: {e}")
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    Décorateur pour retry automatique en cas d'échec
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"All retries failed for {func.__name__}: {e}")
            
            raise last_exception
        
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}")
                        import time
                        time.sleep(delay)
                    else:
                        logger.error(f"All retries failed for {func.__name__}: {e}")
            
            raise last_exception
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator