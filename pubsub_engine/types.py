"""
Types et structures de données pour le PubSub Engine
Compatible avec les types TypeScript existants
"""

from typing import Dict, Any, Optional, List, Callable, Union
from pydantic import BaseModel
from dataclasses import dataclass
from enum import Enum
import time
import uuid


class DataType(str, Enum):
    """Types de données supportés"""
    CANDLES = "candles"
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    INDICATORS = "indicators"
    SIGNALS = "signals"
    ORDERS = "orders"
    POSITIONS = "positions"
    PORTFOLIO = "portfolio"
    ALERTS = "alerts"
    LOGS = "logs"
    SYSTEM = "system"


@dataclass
class SubscriptionPattern:
    """Pattern de souscription pour filtrer les messages"""
    service: str
    data_type: Union[str, List[str]]
    symbol: Optional[str] = "*"
    timeframe: Optional[str] = "*"
    filters: Optional[Dict[str, Any]] = None
    
    def __hash__(self):
        """Rend le pattern hashable"""
        data_type_str = str(self.data_type) if isinstance(self.data_type, list) else self.data_type
        filters_str = str(sorted(self.filters.items())) if self.filters else ""
        return hash((self.service, data_type_str, self.symbol, self.timeframe, filters_str))
    
    def __eq__(self, other):
        """Égalité des patterns"""
        if isinstance(other, SubscriptionPattern):
            return (self.service == other.service and 
                   self.data_type == other.data_type and
                   self.symbol == other.symbol and
                   self.timeframe == other.timeframe and
                   self.filters == other.filters)
        return False


@dataclass
class SubMetadata:
    """Métadonnées reçues avec un message"""
    channel: str
    timestamp: int
    message_id: str
    receive_timestamp: int
    latency: int


@dataclass
class PubMetadata:
    """Métadonnées pour publier un message"""
    persistent: bool = True
    ttl: Optional[int] = None
    priority: int = 0
    retry_count: int = 3


class PubMessage(BaseModel):
    """Message publié dans le système"""
    channel: str
    data: Dict[str, Any]
    timestamp: int
    message_id: str
    metadata: PubMetadata
    
    class Config:
        arbitrary_types_allowed = True


@dataclass
class Subscription:
    """Représente une souscription active"""
    id: str
    pattern: SubscriptionPattern
    callback: Callable[[Dict[str, Any], SubMetadata], None]
    created_at: int
    is_active: bool = True
    
    def __hash__(self):
        """Rend la subscription hashable pour les sets"""
        return hash(self.id)
    
    def __eq__(self, other):
        """Égalité basée sur l'ID"""
        if isinstance(other, Subscription):
            return self.id == other.id
        return False


class ChannelConfig(BaseModel):
    """Configuration d'un canal"""
    name: str
    pattern: str
    description: Optional[str] = None
    retention_time: Optional[int] = None
    max_messages: Optional[int] = None


class RedisConfig(BaseModel):
    """Configuration Redis"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    username: Optional[str] = None
    ssl: bool = False
    ssl_cert_reqs: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None


class WebSocketConfig(BaseModel):
    """Configuration WebSocket"""
    port: int = 8080
    path: str = "/ws"
    host: str = "0.0.0.0"


class PubSubConfig(BaseModel):
    """Configuration principale du PubSub Engine"""
    redis: RedisConfig
    websocket: Optional[WebSocketConfig] = None
    persistence: bool = True
    max_retries: int = 3
    default_ttl: int = 3600000  # 1 heure en ms
    
    
class SubscriptionInfo(BaseModel):
    """Informations sur une souscription"""
    id: str
    pattern: SubscriptionPattern
    created_at: int
    is_active: bool
    message_count: int = 0


class Channel(BaseModel):
    """Représente un canal"""
    id: str
    config: ChannelConfig
    subscription_count: int = 0


# Fonctions utilitaires
def generate_id() -> str:
    """Génère un ID unique"""
    return str(uuid.uuid4())


def current_timestamp() -> int:
    """Timestamp actuel en millisecondes"""
    return int(time.time() * 1000)


def create_subscription_pattern(
    service: str,
    data_type: Union[str, DataType, List[str]], 
    symbol: str = "*",
    timeframe: str = "*",
    **filters
) -> SubscriptionPattern:
    """Crée un pattern de souscription"""
    if isinstance(data_type, DataType):
        data_type = data_type.value
    elif isinstance(data_type, list):
        data_type = [dt.value if isinstance(dt, DataType) else dt for dt in data_type]
    
    return SubscriptionPattern(
        service=service,
        data_type=data_type,
        symbol=symbol,
        timeframe=timeframe,
        filters=filters if filters else None
    )