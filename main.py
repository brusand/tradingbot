#!/usr/bin/env python3
"""
Trading Bot Backend - Main Entry Point
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

from config.settings import settings
from data.persistence import DatabaseManager
from core.session_manager import SessionManager
from core.strategy_engine import StrategyEngine
from core.paper_trading import PaperTradingEngine


class TradingBot:
    def __init__(self):
        self.setup_logging()
        
        self.db_manager = DatabaseManager(settings.database.path)
        self.session_manager = SessionManager(self.db_manager)
        self.strategy_engine = StrategyEngine(self.db_manager)
        self.paper_trading = PaperTradingEngine(self.db_manager, settings.trading.default_initial_balance)
        
        self.running = False
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self):
        """Configure logging based on settings"""
        level = getattr(logging, settings.logging.level.upper())
        
        # Configure root logger
        logging.basicConfig(
            level=level,
            format=settings.logging.format,
            handlers=[]
        )
        
        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(logging.Formatter(settings.logging.format))
        logging.getLogger().addHandler(console_handler)
        
        # Add file handler if specified
        if settings.logging.file_path:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                settings.logging.file_path,
                maxBytes=settings.logging.max_file_size,
                backupCount=settings.logging.backup_count
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(settings.logging.format))
            logging.getLogger().addHandler(file_handler)
    
    async def initialize(self):
        """Initialize all components"""
        self.logger.info("Initializing Trading Bot...")
        
        # Validate settings
        settings.validate()
        
        # Initialize database
        await self.db_manager.init_db()
        
        # Initialize session manager
        await self.session_manager.initialize()
        
        self.logger.info("Trading Bot initialized successfully")
    
    async def start(self):
        """Start the trading bot"""
        await self.initialize()
        
        self.running = True
        self.logger.info("Trading Bot started")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # Main event loop
            while self.running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            await self.shutdown()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self.running = False
    
    async def shutdown(self):
        """Shutdown the trading bot gracefully"""
        self.logger.info("Shutting down Trading Bot...")
        
        # Stop all strategies
        await self.strategy_engine.shutdown()
        
        self.logger.info("Trading Bot shutdown complete")
    
    async def run_cli_mode(self):
        """Run in CLI mode (for development/testing)"""
        await self.initialize()
        
        # Import and run CLI
        from interfaces.cli import cli
        try:
            cli()
        except Exception as e:
            self.logger.error(f"CLI error: {e}")


async def main():
    """Main entry point"""
    bot = TradingBot()
    
    # Check if running in CLI mode
    if len(sys.argv) > 1:
        await bot.run_cli_mode()
    else:
        await bot.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown initiated by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)