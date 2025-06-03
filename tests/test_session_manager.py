import pytest
import pytest_asyncio
import asyncio
from datetime import datetime
import tempfile
import os

from data.models import SessionMode, StrategyConfig, RiskConfig, SessionStatus
from data.persistence import DatabaseManager
from core.session_manager import SessionManager


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
async def session_manager(setup_db):
    """Create session manager with test database"""
    db_manager = setup_db
    manager = SessionManager(db_manager)
    await manager.initialize()
    return manager


@pytest.fixture
def sample_strategy_config():
    """Sample strategy configuration"""
    return StrategyConfig(
        name="TestStrategy",
        parameters={"param1": "value1", "param2": 42},
        timeframe="1h",
        pairs=["BTCUSD", "ETHUSD"]
    )


@pytest.fixture
def sample_risk_config():
    """Sample risk configuration"""
    return RiskConfig(
        max_position_size=0.1,
        stop_loss_pct=2.0,
        take_profit_pct=4.0,
        max_daily_loss=5.0,
        max_exposure_pct=50.0
    )


class TestSessionManager:
    
    @pytest.mark.asyncio
    async def test_create_session(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session creation"""
        session = await session_manager.create_session(
            name="Test Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        assert session is not None
        assert session.name == "Test Session"
        assert session.mode == SessionMode.PAPER
        assert session.status == SessionStatus.CREATED
        assert session.strategy.name == "TestStrategy"
        assert len(session.id) > 0
    
    @pytest.mark.asyncio
    async def test_start_session(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session start"""
        session = await session_manager.create_session(
            name="Test Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        success = await session_manager.start_session(session.id)
        assert success is True
        
        updated_session = await session_manager.get_session(session.id)
        assert updated_session.status == SessionStatus.RUNNING
    
    @pytest.mark.asyncio
    async def test_pause_session(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session pause"""
        session = await session_manager.create_session(
            name="Test Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        await session_manager.start_session(session.id)
        success = await session_manager.pause_session(session.id)
        assert success is True
        
        updated_session = await session_manager.get_session(session.id)
        assert updated_session.status == SessionStatus.PAUSED
    
    @pytest.mark.asyncio
    async def test_stop_session(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session stop"""
        session = await session_manager.create_session(
            name="Test Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        await session_manager.start_session(session.id)
        success = await session_manager.stop_session(session.id)
        assert success is True
        
        updated_session = await session_manager.get_session(session.id)
        assert updated_session.status == SessionStatus.STOPPED
    
    @pytest.mark.asyncio
    async def test_list_sessions(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test listing sessions"""
        # Create multiple sessions
        session1 = await session_manager.create_session(
            name="Session 1",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        session2 = await session_manager.create_session(
            name="Session 2",
            mode=SessionMode.SANDBOX,
            strategy=sample_strategy_config
        )
        
        sessions = await session_manager.list_sessions()
        assert len(sessions) == 2
        
        session_names = [s.name for s in sessions]
        assert "Session 1" in session_names
        assert "Session 2" in session_names
    
    @pytest.mark.asyncio
    async def test_duplicate_session(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session duplication"""
        original = await session_manager.create_session(
            name="Original Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        duplicate = await session_manager.duplicate_session(original.id, "Duplicate Session")
        
        assert duplicate is not None
        assert duplicate.id != original.id
        assert duplicate.name == "Duplicate Session"
        assert duplicate.mode == original.mode
        assert duplicate.strategy.name == original.strategy.name
        assert duplicate.status == SessionStatus.CREATED
    
    @pytest.mark.asyncio
    async def test_delete_session(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session deletion"""
        session = await session_manager.create_session(
            name="Test Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        success = await session_manager.delete_session(session.id)
        assert success is True
        
        deleted_session = await session_manager.get_session(session.id)
        assert deleted_session is None
    
    @pytest.mark.asyncio
    async def test_update_session_state(self, session_manager, sample_strategy_config, sample_risk_config):
        """Test session state update"""
        session = await session_manager.create_session(
            name="Test Session",
            mode=SessionMode.PAPER,
            strategy=sample_strategy_config
        )
        
        state_update = {"test_key": "test_value", "counter": 42}
        success = await session_manager.update_session_state(session.id, state_update)
        assert success is True
        
        updated_session = await session_manager.get_session(session.id)
        assert updated_session.state["test_key"] == "test_value"
        assert updated_session.state["counter"] == 42


if __name__ == "__main__":
    pytest.main([__file__])