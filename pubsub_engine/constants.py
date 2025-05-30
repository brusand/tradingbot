"""
Constantes pour le PubSub Engine
Compatible avec les constantes TypeScript existantes
"""

from .types import PubMetadata


# Canaux prédéfinis - compatibles avec le backend TypeScript
class CHANNELS:
    """Constantes des canaux de communication"""
    
    # Market Data
    MARKET_DATA_CANDLES = "market-data:candles:{symbol}:{timeframe}"
    MARKET_DATA_TRADES = "market-data:trades:{symbol}"
    MARKET_DATA_ORDERBOOK = "market-data:orderbook:{symbol}"
    MARKET_DATA_TICKER = "market-data:ticker:{symbol}"
    
    # Indicators
    INDICATORS_UPDATE = "indicators:update:{symbol}:{timeframe}:{indicator}"
    INDICATORS_BATCH = "indicators:batch:{symbol}:{timeframe}"
    
    # Strategies
    STRATEGIES_SIGNAL = "strategies:signal:{strategyId}:{symbol}"
    STRATEGIES_UPDATE = "strategies:update:{strategyId}"
    STRATEGIES_BACKTEST = "strategies:backtest:{strategyId}"
    
    # Execution
    EXECUTION_ORDER = "execution:order:{orderId}"
    EXECUTION_TRADE = "execution:trade:{orderId}"
    EXECUTION_POSITION = "execution:position:{symbol}"
    
    # Portfolio
    PORTFOLIO_UPDATE = "portfolio:update:{userId}"
    PORTFOLIO_BALANCE = "portfolio:balance:{userId}"
    PORTFOLIO_PNL = "portfolio:pnl:{userId}"
    
    # Risk Management
    RISK_ALERT = "risk:alert:{severity}:{type}"
    RISK_LIMIT = "risk:limit:{userId}:{type}"
    
    # Notifications
    NOTIFICATIONS_USER = "notifications:user:{userId}"
    NOTIFICATIONS_SYSTEM = "notifications:system:{type}"
    NOTIFICATIONS_ALERT = "notifications:alert:{severity}"
    
    # System
    SYSTEM_STATUS = "system:status"
    SYSTEM_METRICS = "system:metrics"
    SYSTEM_LOGS = "system:logs:{severity}"
    
    # WebSocket
    WS_CONNECTION = "websocket:connection"
    WS_SUBSCRIPTION = "websocket:subscription:{userId}"


# Options par défaut pour la publication
DEFAULT_PUB_OPTIONS = PubMetadata(
    persistent=True,
    ttl=3600000,  # 1 heure
    priority=0,
    retry_count=3
)


# Configuration des services
SERVICE_NAMES = {
    "MARKET_DATA": "market-data",
    "INDICATORS": "indicators", 
    "STRATEGIES": "strategies",
    "EXECUTION": "execution",
    "PORTFOLIO": "portfolio",
    "RISK": "risk",
    "NOTIFICATIONS": "notifications",
    "SYSTEM": "system"
}


# Types de données par service
SERVICE_DATA_TYPES = {
    SERVICE_NAMES["MARKET_DATA"]: ["candles", "trades", "orderbook", "ticker"],
    SERVICE_NAMES["INDICATORS"]: ["update", "batch", "alert"],
    SERVICE_NAMES["STRATEGIES"]: ["signal", "update", "backtest", "optimization"],
    SERVICE_NAMES["EXECUTION"]: ["order", "trade", "position", "balance"],
    SERVICE_NAMES["PORTFOLIO"]: ["update", "balance", "pnl", "allocation"],
    SERVICE_NAMES["RISK"]: ["alert", "limit", "breach", "report"],
    SERVICE_NAMES["NOTIFICATIONS"]: ["user", "system", "alert", "email"],
    SERVICE_NAMES["SYSTEM"]: ["status", "metrics", "logs", "health"]
}


# Timeframes supportés
TIMEFRAMES = [
    "1s", "5s", "15s", "30s",
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M"
]


# Symboles populaires pour les tests
POPULAR_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "ADA/USDT", "XRP/USDT",
    "SOL/USDT", "DOT/USDT", "DOGE/USDT", "AVAX/USDT", "LUNA/USDT"
]


# Niveaux de priorité
class PRIORITY:
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


# Niveaux de sévérité pour les alertes
class SEVERITY:
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# Configuration par défaut
DEFAULT_CONFIG = {
    "redis": {
        "host": "localhost",
        "port": 6379,
        "db": 0
    },
    "websocket": {
        "port": 8080,
        "path": "/ws",
        "host": "0.0.0.0"
    },
    "persistence": True,
    "max_retries": 3,
    "default_ttl": 3600000
}