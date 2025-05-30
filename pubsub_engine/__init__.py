"""
PubSub Engine - Module réutilisable pour pub/sub avec Redis
Compatible avec l'architecture TypeScript existante
"""

from .engine import PubSubEngine
from .decorators import Subscribe, Publish, AutoUnsubscribe
from .constants import CHANNELS, DEFAULT_PUB_OPTIONS
from .types import (
    SubscriptionPattern,
    PubMessage,
    SubMetadata,
    PubMetadata,
    Subscription,
    ChannelConfig,
    PubSubConfig,
    RedisConfig,
    WebSocketConfig,
    create_subscription_pattern,
    generate_id,
    current_timestamp
)

__version__ = "1.0.0"
__all__ = [
    "PubSubEngine",
    "Subscribe", 
    "Publish",
    "AutoUnsubscribe",
    "CHANNELS",
    "DEFAULT_PUB_OPTIONS",
    "SubscriptionPattern",
    "PubMessage",
    "SubMetadata", 
    "PubMetadata",
    "Subscription",
    "ChannelConfig",
    "PubSubConfig",
    "RedisConfig",
    "WebSocketConfig",
    "create_subscription_pattern",
    "generate_id",
    "current_timestamp"
]