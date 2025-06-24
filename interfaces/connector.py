from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Callable, Any


class Connector(ABC):
    """Interface abstraite pour les connecteurs d'exchange"""
    
    def __init__(self, api_key: str = "", api_secret: str = "", sandbox: bool = True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
    
    @abstractmethod
    async def __aenter__(self):
        """Contexte manager d'entrée"""
        pass
    
    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Contexte manager de sortie"""
        pass
    
    # Public API methods
    @abstractmethod
    async def get_server_time(self) -> Dict:
        """Récupère l'heure du serveur"""
        pass
    
    @abstractmethod
    async def get_asset_info(self) -> Dict:
        """Récupère les informations des actifs"""
        pass
    
    @abstractmethod
    async def get_tradable_pairs(self) -> Dict:
        """Récupère les paires tradables"""
        pass
    
    @abstractmethod
    async def get_ticker(self, pair: str = None) -> Dict:
        """Récupère le ticker pour une paire"""
        pass
    
    @abstractmethod
    async def get_orderbook(self, pair: str, count: int = 100) -> Dict:
        """Récupère le carnet d'ordres"""
        pass
    
    @abstractmethod
    async def get_recent_trades(self, pair: str, since: str = None) -> Dict:
        """Récupère les trades récents"""
        pass
    
    @abstractmethod
    async def get_ohlc(self, pair: str, interval: int = 1, since: str = None) -> Dict:
        """Récupère les données OHLC"""
        pass
    
    # Private API methods
    @abstractmethod
    async def get_account_balance(self) -> Dict:
        """Récupère le solde du compte"""
        pass
    
    @abstractmethod
    async def get_open_orders(self) -> Dict:
        """Récupère les ordres ouverts"""
        pass
    
    @abstractmethod
    async def get_closed_orders(self) -> Dict:
        """Récupère les ordres fermés"""
        pass
    
    @abstractmethod
    async def get_trades_history(self) -> Dict:
        """Récupère l'historique des trades"""
        pass
    
    @abstractmethod
    async def place_order(
        self, 
        pair: str, 
        type_: str, 
        ordertype: str, 
        volume: str, 
        price: str = None,
        price2: str = None,
        leverage: str = None,
        validate: bool = False
    ) -> Dict:
        """Place un ordre"""
        pass
    
    @abstractmethod
    async def cancel_order(self, txid: str) -> Dict:
        """Annule un ordre"""
        pass
    
    # WebSocket methods
    @abstractmethod
    async def connect_websocket(self):
        """Connecte le WebSocket"""
        pass
    
    @abstractmethod
    async def subscribe_ticker(self, pairs: List[str], callback: Callable):
        """Souscrit aux tickers via WebSocket"""
        pass
    
    @abstractmethod
    async def subscribe_orderbook(self, pairs: List[str], depth: int, callback: Callable):
        """Souscrit au carnet d'ordres via WebSocket"""
        pass
    
    @abstractmethod
    async def subscribe_trades(self, pairs: List[str], callback: Callable):
        """Souscrit aux trades via WebSocket"""
        pass
    
    @abstractmethod
    async def disconnect_websocket(self):
        """Déconnecte le WebSocket"""
        pass