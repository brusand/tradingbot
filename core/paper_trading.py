import uuid
from typing import Dict, List, Optional
from datetime import datetime, UTC
from data.models import Trade, SessionMode
from data.persistence import DatabaseManager


class PaperTradingEngine:
    def __init__(self, db_manager: DatabaseManager, initial_balance: float = 10000.0):
        self.db_manager = db_manager
        self.initial_balance = initial_balance
        self.balances: Dict[str, Dict[str, float]] = {}  # session_id -> {asset: balance}
        self.open_orders: Dict[str, List[Dict]] = {}  # session_id -> [orders]
        self.market_prices: Dict[str, float] = {}  # symbol -> current_price
        
    def initialize_session_balance(self, session_id: str, base_currency: str = "USD"):
        """Initialize paper trading balance for a session"""
        if session_id not in self.balances:
            self.balances[session_id] = {base_currency: self.initial_balance}
        if session_id not in self.open_orders:
            self.open_orders[session_id] = []
    
    def update_market_price(self, symbol: str, price: float):
        """Update current market price for a symbol"""
        self.market_prices[symbol] = price
    
    async def place_order(
        self, 
        session_id: str, 
        symbol: str, 
        side: str, 
        amount: float, 
        order_type: str = "market",
        price: Optional[float] = None
    ) -> Optional[str]:
        """Place a paper trading order"""
        
        if session_id not in self.balances:
            self.initialize_session_balance(session_id)
        
        order_id = str(uuid.uuid4())
        current_price = self.market_prices.get(symbol, 0.0)
        
        if order_type == "market":
            # Execute market order immediately
            if side == "buy":
                return await self._execute_buy_order(session_id, symbol, amount, current_price, order_id)
            else:
                return await self._execute_sell_order(session_id, symbol, amount, current_price, order_id)
        
        elif order_type == "limit":
            # Add limit order to open orders
            order = {
                "id": order_id,
                "session_id": session_id,
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "price": price,
                "type": "limit",
                "timestamp": datetime.now(UTC),
                "status": "pending"
            }
            
            self.open_orders[session_id].append(order)
            return order_id
        
        return None
    
    async def _execute_buy_order(self, session_id: str, symbol: str, amount: float, price: float, order_id: str) -> Optional[str]:
        """Execute a buy order in paper trading"""
        base_asset, quote_asset = self._parse_symbol(symbol)
        cost = amount * price
        
        # Check if we have enough balance
        quote_balance = self.balances[session_id].get(quote_asset, 0.0)
        if quote_balance < cost:
            return None  # Insufficient funds
        
        # Update balances
        self.balances[session_id][quote_asset] -= cost
        if base_asset not in self.balances[session_id]:
            self.balances[session_id][base_asset] = 0.0
        self.balances[session_id][base_asset] += amount
        
        # Create trade record
        trade = Trade(
            id=order_id,
            session_id=session_id,
            symbol=symbol,
            side="buy",
            amount=amount,
            price=price,
            timestamp=datetime.now(UTC),
            status="filled"
        )
        
        await self.db_manager.save_trade(trade)
        return order_id
    
    async def _execute_sell_order(self, session_id: str, symbol: str, amount: float, price: float, order_id: str) -> Optional[str]:
        """Execute a sell order in paper trading"""
        base_asset, quote_asset = self._parse_symbol(symbol)
        
        # Check if we have enough of the base asset
        base_balance = self.balances[session_id].get(base_asset, 0.0)
        if base_balance < amount:
            return None  # Insufficient assets
        
        # Update balances
        proceeds = amount * price
        self.balances[session_id][base_asset] -= amount
        if quote_asset not in self.balances[session_id]:
            self.balances[session_id][quote_asset] = 0.0
        self.balances[session_id][quote_asset] += proceeds
        
        # Create trade record
        trade = Trade(
            id=order_id,
            session_id=session_id,
            symbol=symbol,
            side="sell",
            amount=amount,
            price=price,
            timestamp=datetime.now(UTC),
            status="filled"
        )
        
        await self.db_manager.save_trade(trade)
        return order_id
    
    def _parse_symbol(self, symbol: str) -> tuple:
        """Parse trading symbol into base and quote assets"""
        # Simple implementation - adjust based on actual symbol format
        if "USD" in symbol:
            base = symbol.replace("USD", "")
            quote = "USD"
        elif "BTC" in symbol and symbol != "BTC":
            base = symbol.replace("BTC", "")
            quote = "BTC"
        elif "ETH" in symbol and symbol != "ETH":
            base = symbol.replace("ETH", "")
            quote = "ETH"
        else:
            # Default case
            base = symbol[:3]
            quote = symbol[3:]
        
        return base, quote
    
    async def check_and_execute_limit_orders(self, symbol: str, current_price: float):
        """Check and execute limit orders when price conditions are met"""
        for session_id, orders in self.open_orders.items():
            executed_orders = []
            
            for order in orders:
                if order["symbol"] == symbol and order["status"] == "pending":
                    should_execute = False
                    
                    if order["side"] == "buy" and current_price <= order["price"]:
                        should_execute = True
                    elif order["side"] == "sell" and current_price >= order["price"]:
                        should_execute = True
                    
                    if should_execute:
                        if order["side"] == "buy":
                            result = await self._execute_buy_order(
                                session_id, symbol, order["amount"], current_price, order["id"]
                            )
                        else:
                            result = await self._execute_sell_order(
                                session_id, symbol, order["amount"], current_price, order["id"]
                            )
                        
                        if result:
                            order["status"] = "filled"
                            executed_orders.append(order)
            
            # Remove executed orders
            for executed_order in executed_orders:
                if executed_order in self.open_orders[session_id]:
                    self.open_orders[session_id].remove(executed_order)
    
    def get_balance(self, session_id: str) -> Dict[str, float]:
        """Get current balance for a session"""
        return self.balances.get(session_id, {})
    
    def get_open_orders(self, session_id: str) -> List[Dict]:
        """Get open orders for a session"""
        return self.open_orders.get(session_id, [])
    
    async def cancel_order(self, session_id: str, order_id: str) -> bool:
        """Cancel an open order"""
        if session_id not in self.open_orders:
            return False
        
        for order in self.open_orders[session_id]:
            if order["id"] == order_id and order["status"] == "pending":
                order["status"] = "cancelled"
                self.open_orders[session_id].remove(order)
                return True
        
        return False
    
    def calculate_portfolio_value(self, session_id: str) -> float:
        """Calculate total portfolio value in base currency (USD)"""
        if session_id not in self.balances:
            return 0.0
        
        total_value = 0.0
        balances = self.balances[session_id]
        
        for asset, amount in balances.items():
            if asset == "USD":
                total_value += amount
            else:
                # Convert to USD using current market price
                symbol = f"{asset}USD"
                price = self.market_prices.get(symbol, 0.0)
                total_value += amount * price
        
        return total_value
    
    def get_portfolio_summary(self, session_id: str) -> Dict:
        """Get comprehensive portfolio summary"""
        if session_id not in self.balances:
            return {}
        
        balances = self.balances[session_id]
        portfolio_value = self.calculate_portfolio_value(session_id)
        pnl = portfolio_value - self.initial_balance
        pnl_pct = (pnl / self.initial_balance) * 100 if self.initial_balance > 0 else 0
        
        return {
            "balances": balances,
            "portfolio_value": portfolio_value,
            "initial_balance": self.initial_balance,
            "pnl": pnl,
            "pnl_percentage": pnl_pct,
            "open_orders_count": len(self.open_orders.get(session_id, []))
        }