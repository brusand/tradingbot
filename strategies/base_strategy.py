from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime, UTC
from data.models import Trade, StrategyConfig
from connectors.kraken_connector import KrakenConnector


class BaseStrategy(ABC):
    def __init__(self, config: StrategyConfig, connector: KrakenConnector):
        self.config = config
        self.connector = connector
        self.positions: Dict[str, float] = {}
        self.is_running = False
        self.last_update = datetime.now(UTC)
        
    @abstractmethod
    async def initialize(self):
        """Initialize strategy parameters and state"""
        pass
    
    @abstractmethod
    async def on_market_data(self, symbol: str, data: Dict[str, Any]):
        """Process incoming market data"""
        pass
    
    @abstractmethod
    async def should_buy(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Determine if we should place a buy order"""
        pass
    
    @abstractmethod
    async def should_sell(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Determine if we should place a sell order"""
        pass
    
    @abstractmethod
    async def calculate_position_size(self, symbol: str, price: float) -> float:
        """Calculate position size for a trade"""
        pass
    
    async def start(self):
        """Start the strategy"""
        self.is_running = True
        await self.initialize()
        
    async def stop(self):
        """Stop the strategy"""
        self.is_running = False
        
    async def update_position(self, symbol: str, amount: float):
        """Update position for a symbol"""
        if symbol not in self.positions:
            self.positions[symbol] = 0.0
        self.positions[symbol] += amount
        
    def get_position(self, symbol: str) -> float:
        """Get current position for a symbol"""
        return self.positions.get(symbol, 0.0)
    
    async def place_buy_order(self, symbol: str, amount: float, price: Optional[float] = None) -> Optional[str]:
        """Place a buy order"""
        try:
            if price:
                order = await self.connector.place_order(
                    pair=symbol,
                    type_="buy",
                    ordertype="limit",
                    volume=str(amount),
                    price=str(price)
                )
            else:
                order = await self.connector.place_order(
                    pair=symbol,
                    type_="buy",
                    ordertype="market",
                    volume=str(amount)
                )
            
            if order.get("error"):
                return None
                
            return order["result"]["txid"][0] if "result" in order else None
            
        except Exception as e:
            print(f"Error placing buy order: {e}")
            return None
    
    async def place_sell_order(self, symbol: str, amount: float, price: Optional[float] = None) -> Optional[str]:
        """Place a sell order"""
        try:
            if price:
                order = await self.connector.place_order(
                    pair=symbol,
                    type_="sell",
                    ordertype="limit",
                    volume=str(amount),
                    price=str(price)
                )
            else:
                order = await self.connector.place_order(
                    pair=symbol,
                    type_="sell",
                    ordertype="market",
                    volume=str(amount)
                )
            
            if order.get("error"):
                return None
                
            return order["result"]["txid"][0] if "result" in order else None
            
        except Exception as e:
            print(f"Error placing sell order: {e}")
            return None


class SimpleMovingAverageStrategy(BaseStrategy):
    def __init__(self, config: StrategyConfig, connector: KrakenConnector):
        super().__init__(config, connector)
        self.short_window = config.parameters.get("short_window", 10)
        self.long_window = config.parameters.get("long_window", 30)
        self.price_history: Dict[str, List[float]] = {}
        
    async def initialize(self):
        """Initialize SMA strategy"""
        for pair in self.config.pairs:
            self.price_history[pair] = []
            
    async def on_market_data(self, symbol: str, data: Dict[str, Any]):
        """Process market data and update price history"""
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            
        # Extract price from market data (adjust based on actual data format)
        price = data.get("price", 0.0)
        if price > 0:
            self.price_history[symbol].append(price)
            
            # Keep only required history
            max_window = max(self.short_window, self.long_window)
            if len(self.price_history[symbol]) > max_window:
                self.price_history[symbol] = self.price_history[symbol][-max_window:]
            
            # Check for trading signals
            if await self.should_buy(symbol, data):
                position_size = await self.calculate_position_size(symbol, price)
                await self.place_buy_order(symbol, position_size)
                
            elif await self.should_sell(symbol, data):
                current_position = self.get_position(symbol)
                if current_position > 0:
                    await self.place_sell_order(symbol, current_position)
    
    def _calculate_sma(self, prices: List[float], window: int) -> Optional[float]:
        """Calculate Simple Moving Average"""
        if len(prices) < window:
            return None
        return sum(prices[-window:]) / window
    
    async def should_buy(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Buy when short SMA crosses above long SMA"""
        prices = self.price_history.get(symbol, [])
        if len(prices) < self.long_window:
            return False
            
        short_sma = self._calculate_sma(prices, self.short_window)
        long_sma = self._calculate_sma(prices, self.long_window)
        
        if short_sma is None or long_sma is None:
            return False
            
        # Check if we don't already have a position
        current_position = self.get_position(symbol)
        
        return short_sma > long_sma and current_position == 0
    
    async def should_sell(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Sell when short SMA crosses below long SMA"""
        prices = self.price_history.get(symbol, [])
        if len(prices) < self.long_window:
            return False
            
        short_sma = self._calculate_sma(prices, self.short_window)
        long_sma = self._calculate_sma(prices, self.long_window)
        
        if short_sma is None or long_sma is None:
            return False
            
        # Check if we have a position to sell
        current_position = self.get_position(symbol)
        
        return short_sma < long_sma and current_position > 0
    
    async def calculate_position_size(self, symbol: str, price: float) -> float:
        """Calculate position size based on available balance"""
        # This is a simplified implementation
        # In practice, you'd get account balance and apply risk management
        max_position = self.config.parameters.get("max_position_size", 0.1)
        return max_position