import pytest
import pytest_asyncio
import tempfile
import os

from data.persistence import DatabaseManager
from core.paper_trading import PaperTradingEngine


@pytest_asyncio.fixture
async def setup_db():
    """Setup test database"""
    temp_db = tempfile.NamedTemporaryFile(delete=False)
    temp_db.close()
    
    db_manager = DatabaseManager(temp_db.name)
    await db_manager.init_db()
    
    yield db_manager
    
    # Cleanup
    os.unlink(temp_db.name)


@pytest_asyncio.fixture
async def paper_trading(setup_db):
    """Create paper trading engine with test database"""
    db_manager = setup_db
    engine = PaperTradingEngine(db_manager, initial_balance=10000.0)
    return engine


class TestPaperTradingEngine:
    
    def test_initialize_session_balance(self, paper_trading):
        """Test session balance initialization"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        
        balance = paper_trading.get_balance(session_id)
        assert balance["USD"] == 10000.0
    
    def test_update_market_price(self, paper_trading):
        """Test market price update"""
        paper_trading.update_market_price("BTCUSD", 50000.0)
        assert paper_trading.market_prices["BTCUSD"] == 50000.0
    
    @pytest.mark.asyncio
    async def test_buy_order_execution(self, paper_trading):
        """Test buy order execution"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        order_id = await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="buy",
            amount=0.1,
            order_type="market"
        )
        
        assert order_id is not None
        
        balance = paper_trading.get_balance(session_id)
        assert balance["USD"] == 5000.0  # 10000 - (0.1 * 50000)
        assert balance["BTC"] == 0.1
    
    @pytest.mark.asyncio
    async def test_sell_order_execution(self, paper_trading):
        """Test sell order execution"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        # First buy some BTC
        await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="buy",
            amount=0.1,
            order_type="market"
        )
        
        # Update price and sell
        paper_trading.update_market_price("BTCUSD", 55000.0)
        
        order_id = await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="sell",
            amount=0.1,
            order_type="market"
        )
        
        assert order_id is not None
        
        balance = paper_trading.get_balance(session_id)
        assert balance["USD"] == 10500.0  # 5000 + (0.1 * 55000)
        assert balance.get("BTC", 0.0) == 0.0
    
    @pytest.mark.asyncio
    async def test_insufficient_funds(self, paper_trading):
        """Test order rejection with insufficient funds"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        # Try to buy more than balance allows
        order_id = await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="buy",
            amount=1.0,  # Would cost 50000, but only have 10000
            order_type="market"
        )
        
        assert order_id is None
        
        balance = paper_trading.get_balance(session_id)
        assert balance["USD"] == 10000.0  # Balance unchanged
    
    @pytest.mark.asyncio
    async def test_limit_order_placement(self, paper_trading):
        """Test limit order placement"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        order_id = await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="buy",
            amount=0.1,
            order_type="limit",
            price=48000.0
        )
        
        assert order_id is not None
        
        open_orders = paper_trading.get_open_orders(session_id)
        assert len(open_orders) == 1
        assert open_orders[0]["price"] == 48000.0
        assert open_orders[0]["status"] == "pending"
    
    @pytest.mark.asyncio
    async def test_limit_order_execution(self, paper_trading):
        """Test limit order execution when price condition is met"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        # Place limit buy order
        await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="buy",
            amount=0.1,
            order_type="limit",
            price=48000.0
        )
        
        # Price drops to trigger the order
        await paper_trading.check_and_execute_limit_orders("BTCUSD", 47000.0)
        
        balance = paper_trading.get_balance(session_id)
        assert balance["USD"] == 5300.0  # 10000 - (0.1 * 47000)
        assert balance["BTC"] == 0.1
        
        open_orders = paper_trading.get_open_orders(session_id)
        assert len(open_orders) == 0  # Order should be executed and removed
    
    def test_portfolio_value_calculation(self, paper_trading):
        """Test portfolio value calculation"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        
        # Add some assets manually for testing
        paper_trading.balances[session_id] = {
            "USD": 5000.0,
            "BTC": 0.1
        }
        
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        portfolio_value = paper_trading.calculate_portfolio_value(session_id)
        assert portfolio_value == 10000.0  # 5000 + (0.1 * 50000)
    
    def test_portfolio_summary(self, paper_trading):
        """Test portfolio summary generation"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        
        # Add some assets
        paper_trading.balances[session_id] = {
            "USD": 6000.0,
            "BTC": 0.1
        }
        
        paper_trading.update_market_price("BTCUSD", 50000.0)
        
        summary = paper_trading.get_portfolio_summary(session_id)
        
        assert summary["portfolio_value"] == 11000.0
        assert summary["initial_balance"] == 10000.0
        assert summary["pnl"] == 1000.0
        assert summary["pnl_percentage"] == 10.0
    
    @pytest.mark.asyncio
    async def test_cancel_order(self, paper_trading):
        """Test order cancellation"""
        session_id = "test_session_1"
        paper_trading.initialize_session_balance(session_id)
        
        order_id = await paper_trading.place_order(
            session_id=session_id,
            symbol="BTCUSD",
            side="buy",
            amount=0.1,
            order_type="limit",
            price=48000.0
        )
        
        success = await paper_trading.cancel_order(session_id, order_id)
        assert success is True
        
        open_orders = paper_trading.get_open_orders(session_id)
        assert len(open_orders) == 0


if __name__ == "__main__":
    pytest.main([__file__])