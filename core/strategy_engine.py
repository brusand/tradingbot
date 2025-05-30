import asyncio
from typing import Dict, List, Optional, Type
from datetime import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import logging

from data.models import Session, SessionStatus, Trade
from data.persistence import DatabaseManager
from connectors.kraken_connector import KrakenConnector
from strategies.base_strategy import BaseStrategy, SimpleMovingAverageStrategy


class StrategyEngine:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.active_strategies: Dict[str, BaseStrategy] = {}
        self.strategy_tasks: Dict[str, asyncio.Task] = {}
        self.connectors: Dict[str, KrakenConnector] = {}
        
        # Strategy registry
        self.strategy_registry: Dict[str, Type[BaseStrategy]] = {
            "SimpleMovingAverage": SimpleMovingAverageStrategy
        }
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    async def start_strategy(self, session: Session, api_key: str = "", api_secret: str = "") -> bool:
        """Start a strategy for a session"""
        try:
            if session.id in self.active_strategies:
                self.logger.warning(f"Strategy for session {session.id} is already running")
                return False
            
            # Create connector for this session
            connector = KrakenConnector(
                api_key=api_key, 
                api_secret=api_secret, 
                sandbox=(session.mode.value != "live")
            )
            
            self.connectors[session.id] = connector
            
            # Create strategy instance
            strategy_class = self.strategy_registry.get(session.strategy.name)
            if not strategy_class:
                self.logger.error(f"Unknown strategy: {session.strategy.name}")
                return False
            
            strategy = strategy_class(session.strategy, connector)
            self.active_strategies[session.id] = strategy
            
            # Start the strategy in a separate task
            task = asyncio.create_task(self._run_strategy(session.id, strategy))
            self.strategy_tasks[session.id] = task
            
            self.logger.info(f"Started strategy for session {session.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start strategy for session {session.id}: {e}")
            return False
    
    async def stop_strategy(self, session_id: str) -> bool:
        """Stop a strategy for a session"""
        try:
            if session_id not in self.active_strategies:
                self.logger.warning(f"No active strategy found for session {session_id}")
                return False
            
            # Stop the strategy
            strategy = self.active_strategies[session_id]
            await strategy.stop()
            
            # Cancel the task
            if session_id in self.strategy_tasks:
                task = self.strategy_tasks[session_id]
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                del self.strategy_tasks[session_id]
            
            # Close connector
            if session_id in self.connectors:
                connector = self.connectors[session_id]
                await connector.disconnect_websocket()
                del self.connectors[session_id]
            
            # Remove from active strategies
            del self.active_strategies[session_id]
            
            self.logger.info(f"Stopped strategy for session {session_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop strategy for session {session_id}: {e}")
            return False
    
    async def _run_strategy(self, session_id: str, strategy: BaseStrategy):
        """Run strategy in async loop"""
        try:
            connector = self.connectors[session_id]
            
            # Start the strategy
            await strategy.start()
            
            # Set up WebSocket connections for market data
            async with connector:
                await connector.connect_websocket()
                
                # Subscribe to market data for strategy pairs
                await connector.subscribe_ticker(
                    strategy.config.pairs,
                    lambda data: self._handle_market_data(session_id, strategy, data)
                )
                
                # Keep the strategy running
                while strategy.is_running:
                    await asyncio.sleep(1)  # Check every second
                    
        except asyncio.CancelledError:
            self.logger.info(f"Strategy task for session {session_id} was cancelled")
        except Exception as e:
            self.logger.error(f"Strategy error for session {session_id}: {e}")
            # Mark session as error state
            await self._handle_strategy_error(session_id, str(e))
    
    async def _handle_market_data(self, session_id: str, strategy: BaseStrategy, data: Dict):
        """Handle incoming market data"""
        try:
            # Extract symbol and price data from WebSocket message
            # This is a simplified example - adjust based on actual Kraken WS format
            if isinstance(data, list) and len(data) > 1:
                symbol = data[-1]  # Assuming symbol is in the last element
                price_data = data[1] if len(data) > 1 else {}
                
                await strategy.on_market_data(symbol, price_data)
                
        except Exception as e:
            self.logger.error(f"Error handling market data for session {session_id}: {e}")
    
    async def _handle_strategy_error(self, session_id: str, error_message: str):
        """Handle strategy errors"""
        try:
            # Update session status in database
            session = await self.db_manager.load_session(session_id)
            if session:
                session.status = SessionStatus.ERROR
                session.state["last_error"] = error_message
                session.updated_at = datetime.utcnow()
                await self.db_manager.save_session(session)
                
        except Exception as e:
            self.logger.error(f"Failed to handle strategy error: {e}")
    
    def get_active_strategies(self) -> List[str]:
        """Get list of active strategy session IDs"""
        return list(self.active_strategies.keys())
    
    def is_strategy_running(self, session_id: str) -> bool:
        """Check if strategy is running for a session"""
        return session_id in self.active_strategies
    
    async def get_strategy_status(self, session_id: str) -> Optional[Dict]:
        """Get status information for a strategy"""
        if session_id not in self.active_strategies:
            return None
            
        strategy = self.active_strategies[session_id]
        return {
            "session_id": session_id,
            "is_running": strategy.is_running,
            "last_update": strategy.last_update,
            "positions": strategy.positions,
            "strategy_name": strategy.config.name
        }
    
    async def pause_strategy(self, session_id: str) -> bool:
        """Pause a running strategy"""
        if session_id not in self.active_strategies:
            return False
            
        strategy = self.active_strategies[session_id]
        strategy.is_running = False
        return True
    
    async def resume_strategy(self, session_id: str) -> bool:
        """Resume a paused strategy"""
        if session_id not in self.active_strategies:
            return False
            
        strategy = self.active_strategies[session_id]
        strategy.is_running = True
        return True
    
    async def shutdown(self):
        """Shutdown all strategies and clean up"""
        for session_id in list(self.active_strategies.keys()):
            await self.stop_strategy(session_id)
        
        self.logger.info("Strategy engine shutdown complete")