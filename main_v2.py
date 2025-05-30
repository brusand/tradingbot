#!/usr/bin/env python3
"""
Trading Bot Backend v2.0 - Phase 2 Implementation
Advanced Multi-Session Management with Monitoring
"""

import asyncio
import argparse
import sys
from typing import Optional

# Phase 2 imports
from core.multi_session_manager import MultiSessionManager
from core.process_monitor import ProcessMonitor
from core.session_replay import SessionReplayManager
from core.performance_analytics import PerformanceAnalytics
from data.persistence import DatabaseManager
from config.settings import settings


class TradingBotV2:
    def __init__(self):
        self.db_manager = DatabaseManager(settings.database.path)
        self.multi_session_manager = MultiSessionManager(self.db_manager)
        self.process_monitor = ProcessMonitor(self.db_manager)
        self.replay_manager = SessionReplayManager(self.db_manager)
        self.analytics = PerformanceAnalytics(self.db_manager)
        
    async def initialize(self):
        """Initialize all Phase 2 components"""
        print("Initializing Trading Bot v2.0...")
        await self.multi_session_manager.initialize()
        print("✓ Multi-session manager initialized")
        print("✓ Process monitoring enabled")
        print("✓ Session replay system ready")
        print("✓ Performance analytics ready")
        print(f"✓ Max concurrent sessions: {settings.trading.max_concurrent_strategies}")
        
    async def run_daemon_mode(self):
        """Run in daemon mode with monitoring"""
        print("Starting Trading Bot v2.0 in daemon mode...")
        await self.initialize()
        
        try:
            # Keep running
            while True:
                await asyncio.sleep(10)
                
                # Periodic cleanup
                await self.process_monitor.cleanup_old_alerts(max_age_hours=24)
                await self.replay_manager.cleanup_old_data(max_age_days=30)
                
        except KeyboardInterrupt:
            print("\nShutdown initiated...")
        finally:
            await self.shutdown()
    
    async def run_web_mode(self, host: str = "0.0.0.0", port: int = 8000):
        """Run web UI mode"""
        print(f"Starting Trading Bot v2.0 Web UI on {host}:{port}...")
        await self.initialize()
        
        try:
            from interfaces.web_ui import app
            import uvicorn
            
            # Update global instances in web_ui
            import interfaces.web_ui as web_ui_module
            web_ui_module.db_manager = self.db_manager
            web_ui_module.multi_session_manager = self.multi_session_manager
            
            config = uvicorn.Config(app, host=host, port=port, log_level="info")
            server = uvicorn.Server(config)
            
            print(f"✓ Web UI available at http://{host}:{port}")
            print("✓ WebSocket real-time updates enabled")
            print("✓ REST API endpoints active")
            
            await server.serve()
            
        except KeyboardInterrupt:
            print("\nShutdown initiated...")
        finally:
            await self.shutdown()
    
    async def run_cli_mode(self):
        """Run enhanced CLI mode"""
        print("Trading Bot v2.0 CLI Mode")
        print("Use 'python -m interfaces.cli_v2 --help' for advanced commands")
        
        await self.initialize()
        
        # Import and run CLI v2
        from interfaces.cli_v2 import cli
        try:
            cli()
        except Exception as e:
            print(f"CLI error: {e}")
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        print("Shutting down Trading Bot v2.0...")
        
        try:
            await self.multi_session_manager.shutdown()
            print("✓ Multi-session manager stopped")
            
            await self.process_monitor.stop_monitoring()
            print("✓ Process monitoring stopped")
            
        except Exception as e:
            print(f"Error during shutdown: {e}")
        
        print("✓ Trading Bot v2.0 shutdown complete")


async def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(description="Trading Bot v2.0 - Advanced Multi-Session Management")
    parser.add_argument("--mode", choices=["daemon", "web", "cli"], default="cli",
                       help="Run mode (default: cli)")
    parser.add_argument("--host", default="0.0.0.0", help="Web UI host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Web UI port (default: 8000)")
    parser.add_argument("--version", action="store_true", help="Show version info")
    
    args = parser.parse_args()
    
    if args.version:
        print("Trading Bot v2.0 - Phase 2")
        print("Features:")
        print("  ✓ Multi-session parallel execution")
        print("  ✓ Advanced process monitoring")
        print("  ✓ Session kill/force stop")
        print("  ✓ Real-time monitoring dashboard")
        print("  ✓ Session replay and duplication")
        print("  ✓ Performance analytics")
        print("  ✓ Web UI with WebSocket updates")
        print("  ✓ Enhanced CLI with batch operations")
        return
    
    bot = TradingBotV2()
    
    try:
        if args.mode == "daemon":
            await bot.run_daemon_mode()
        elif args.mode == "web":
            await bot.run_web_mode(args.host, args.port)
        elif args.mode == "cli":
            await bot.run_cli_mode()
        else:
            print(f"Unknown mode: {args.mode}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())