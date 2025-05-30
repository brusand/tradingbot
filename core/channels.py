"""
Définition des canaux PubSub pour le système de trading
"""


class CHANNELS:
    """Constantes pour les canaux PubSub"""
    
    # Canaux de données de marché
    MARKET_DATA = "market.data.{symbol}.{timeframe}"
    MARKET_TICK = "market.tick.{symbol}"
    MARKET_ORDERBOOK = "market.orderbook.{symbol}"
    
    # Canaux des stratégies
    STRATEGIES_SIGNAL = "strategies.signal.{strategyId}.{symbol}"
    STRATEGIES_UPDATE = "strategies.update.{strategyId}"
    STRATEGIES_METRICS = "strategies.metrics.{strategyId}"
    STRATEGIES_STATUS = "strategies.status.{strategyId}"
    
    # Canaux des indicateurs
    INDICATORS_UPDATE = "indicators.update.{symbol}.{timeframe}.{indicator}"
    INDICATORS_REQUEST = "indicators.request.{symbol}.{timeframe}"
    INDICATORS_CALCULATE = "indicators.calculate.{indicator}"
    
    # Canaux de portfolio et balance
    PORTFOLIO_UPDATE = "portfolio.update.{userId}"
    PORTFOLIO_BALANCE = "portfolio.balance.{userId}"
    PORTFOLIO_POSITIONS = "portfolio.positions.{userId}"
    
    # Canaux d'ordres
    ORDERS_CREATE = "orders.create.{userId}"
    ORDERS_UPDATE = "orders.update.{userId}.{orderId}"
    ORDERS_FILL = "orders.fill.{userId}.{orderId}"
    ORDERS_CANCEL = "orders.cancel.{userId}.{orderId}"
    
    # Canaux de risque
    RISK_CHECK = "risk.check.{userId}"
    RISK_ALERT = "risk.alert.{userId}"
    RISK_LIMIT = "risk.limit.{userId}"
    
    # Canaux système
    SYSTEM_STATUS = "system.status"
    SYSTEM_HEALTH = "system.health"
    SYSTEM_METRICS = "system.metrics"
    SYSTEM_ALERTS = "system.alerts"
    
    # Canaux de logging et audit
    AUDIT_TRADES = "audit.trades.{userId}"
    AUDIT_ORDERS = "audit.orders.{userId}"
    AUDIT_SIGNALS = "audit.signals.{strategyId}"
    
    # Canaux de session
    SESSION_START = "session.start.{sessionId}"
    SESSION_STOP = "session.stop.{sessionId}"
    SESSION_UPDATE = "session.update.{sessionId}"
    
    @classmethod
    def get_all_patterns(cls):
        """Retourne tous les patterns de canaux"""
        patterns = []
        for attr_name in dir(cls):
            if not attr_name.startswith('_') and attr_name != 'get_all_patterns':
                attr_value = getattr(cls, attr_name)
                if isinstance(attr_value, str) and '{' in attr_value:
                    patterns.append(attr_value)
        return patterns
    
    @classmethod
    def format_channel(cls, pattern: str, **kwargs):
        """Formate un canal avec les paramètres fournis"""
        try:
            return pattern.format(**kwargs)
        except KeyError as e:
            raise ValueError(f"Missing parameter {e} for channel pattern {pattern}")


# Alias pour la compatibilité
CHANNEL_PATTERNS = CHANNELS