import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Callable
from datetime import datetime
import signal
import psutil
import logging
import json
from dataclasses import dataclass

from data.models import Session, SessionStatus
from data.persistence import DatabaseManager
from core.session_manager import SessionManager
from core.strategy_engine import StrategyEngine
from core.process_monitor import ProcessMonitor
from config.settings import settings


@dataclass
class SessionProcess:
    session_id: str
    process_id: int
    started_at: datetime
    status: str  # running, stopping, stopped, error
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    last_heartbeat: Optional[datetime] = None


class MultiSessionManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.session_manager = SessionManager(db_manager)
        self.process_monitor = ProcessMonitor(db_manager)
        
        # Process management
        self.active_processes: Dict[str, SessionProcess] = {}
        self.process_pool: Optional[ProcessPoolExecutor] = None
        self.max_concurrent = settings.trading.max_concurrent_strategies
        
        # Monitoring
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
        
        # Event callbacks
        self.event_callbacks: Dict[str, List[Callable]] = {
            'session_started': [],
            'session_stopped': [],
            'session_error': [],
            'process_killed': []
        }
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    async def initialize(self):
        """Initialize the multi-session manager"""
        await self.session_manager.initialize()
        
        # Create process pool
        self.process_pool = ProcessPoolExecutor(
            max_workers=self.max_concurrent,
            mp_context=mp.get_context('spawn')
        )
        
        # Start monitoring
        await self.start_monitoring()
        await self.process_monitor.start_monitoring()
        
        self.logger.info(f"MultiSessionManager initialized with max {self.max_concurrent} concurrent sessions")
    
    async def start_sessions(
        self, 
        session_ids: List[str], 
        api_keys: Optional[Dict[str, str]] = None,
        api_secrets: Optional[Dict[str, str]] = None
    ) -> Dict[str, bool]:
        """Start multiple sessions in parallel"""
        if len(session_ids) > self.max_concurrent:
            raise ValueError(f"Cannot start {len(session_ids)} sessions. Max concurrent: {self.max_concurrent}")
        
        # Check current capacity
        available_slots = self.max_concurrent - len(self.active_processes)
        if len(session_ids) > available_slots:
            raise ValueError(f"Not enough capacity. Available slots: {available_slots}")
        
        results = {}
        tasks = []
        
        for session_id in session_ids:
            # Validate session exists and is not already running
            session = await self.session_manager.get_session(session_id)
            if not session:
                results[session_id] = False
                continue
                
            if session_id in self.active_processes:
                self.logger.warning(f"Session {session_id} is already running")
                results[session_id] = False
                continue
            
            # Prepare credentials
            api_key = api_keys.get(session_id, "") if api_keys else ""
            api_secret = api_secrets.get(session_id, "") if api_secrets else ""
            
            # Create task for starting session
            task = asyncio.create_task(
                self._start_single_session(session_id, api_key, api_secret)
            )
            tasks.append((session_id, task))
        
        # Wait for all sessions to start
        for session_id, task in tasks:
            try:
                success = await task
                results[session_id] = success
                if success:
                    self.logger.info(f"Successfully started session {session_id}")
                else:
                    self.logger.error(f"Failed to start session {session_id}")
            except Exception as e:
                self.logger.error(f"Error starting session {session_id}: {e}")
                results[session_id] = False
        
        return results
    
    async def _start_single_session(self, session_id: str, api_key: str, api_secret: str) -> bool:
        """Start a single session in a separate process"""
        try:
            # Update session status
            success = await self.session_manager.start_session(session_id)
            if not success:
                return False
            
            # Submit to process pool
            future = self.process_pool.submit(
                _run_session_process,
                session_id,
                self.db_manager.db_path,
                api_key,
                api_secret
            )
            
            # Get process ID (this is a bit tricky with ProcessPoolExecutor)
            # We'll use a workaround by getting the PID from the worker process
            await asyncio.sleep(0.1)  # Give time for process to start
            
            # For now, we'll track it without exact PID
            # In production, you might want to use a different approach
            process_info = SessionProcess(
                session_id=session_id,
                process_id=0,  # Will be updated by monitoring
                started_at=datetime.utcnow(),
                status="running",
                last_heartbeat=datetime.utcnow()
            )
            
            self.active_processes[session_id] = process_info
            
            # Add to process monitor (we'll get the real PID later)
            # For now, we use a placeholder
            if hasattr(future, '_process') and future._process:
                actual_pid = future._process.pid
                process_info.process_id = actual_pid
                self.process_monitor.add_process(session_id, actual_pid)
            
            # Trigger callbacks
            await self._trigger_event('session_started', session_id)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start session {session_id}: {e}")
            # Update session status to error
            session = await self.session_manager.get_session(session_id)
            if session:
                session.status = SessionStatus.ERROR
                session.state["last_error"] = str(e)
                await self.db_manager.save_session(session)
            return False
    
    async def stop_sessions(self, session_ids: List[str], force: bool = False) -> Dict[str, bool]:
        """Stop multiple sessions"""
        results = {}
        tasks = []
        
        for session_id in session_ids:
            if session_id not in self.active_processes:
                self.logger.warning(f"Session {session_id} is not running")
                results[session_id] = False
                continue
            
            task = asyncio.create_task(
                self._stop_single_session(session_id, force)
            )
            tasks.append((session_id, task))
        
        # Wait for all sessions to stop
        for session_id, task in tasks:
            try:
                success = await task
                results[session_id] = success
            except Exception as e:
                self.logger.error(f"Error stopping session {session_id}: {e}")
                results[session_id] = False
        
        return results
    
    async def _stop_single_session(self, session_id: str, force: bool = False) -> bool:
        """Stop a single session"""
        try:
            process_info = self.active_processes.get(session_id)
            if not process_info:
                return False
            
            if force:
                # Force kill the process
                success = await self._kill_session_process(session_id)
            else:
                # Graceful shutdown
                success = await self.session_manager.stop_session(session_id)
                
                # Wait a bit for graceful shutdown
                await asyncio.sleep(2)
                
                # If still running, force kill
                if session_id in self.active_processes:
                    await self._kill_session_process(session_id)
            
            # Clean up
            if session_id in self.active_processes:
                del self.active_processes[session_id]
            
            # Trigger callbacks
            await self._trigger_event('session_stopped', session_id)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to stop session {session_id}: {e}")
            return False
    
    async def _kill_session_process(self, session_id: str) -> bool:
        """Force kill a session process"""
        try:
            # Use process monitor to kill the process
            success = await self.process_monitor.kill_process(session_id, force=True)
            
            if success:
                self.logger.info(f"Killed process for session {session_id}")
                # Trigger callbacks
                await self._trigger_event('process_killed', session_id)
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to kill process for session {session_id}: {e}")
            return False
    
    async def kill_sessions(self, session_ids: List[str]) -> Dict[str, bool]:
        """Force kill multiple sessions"""
        return await self.stop_sessions(session_ids, force=True)
    
    def list_running_sessions(self) -> List[SessionProcess]:
        """List all currently running sessions"""
        return list(self.active_processes.values())
    
    def get_session_process(self, session_id: str) -> Optional[SessionProcess]:
        """Get process info for a specific session"""
        return self.active_processes.get(session_id)
    
    async def start_monitoring(self):
        """Start the monitoring task"""
        if not self.is_monitoring:
            self.is_monitoring = True
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            self.logger.info("Started session monitoring")
    
    async def stop_monitoring(self):
        """Stop the monitoring task"""
        if self.is_monitoring:
            self.is_monitoring = False
            if self.monitoring_task:
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass
            self.logger.info("Stopped session monitoring")
    
    async def _monitoring_loop(self):
        """Monitor running sessions and update their status"""
        while self.is_monitoring:
            try:
                for session_id, process_info in list(self.active_processes.items()):
                    await self._update_process_stats(session_id, process_info)
                    
                await asyncio.sleep(5)  # Monitor every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)
    
    async def _update_process_stats(self, session_id: str, process_info: SessionProcess):
        """Update process statistics"""
        try:
            if process_info.process_id == 0:
                # Try to find the actual process ID
                # This is a simplified approach - in production you'd want better process tracking
                return
            
            try:
                process = psutil.Process(process_info.process_id)
                
                if not process.is_running():
                    # Process died
                    process_info.status = "stopped"
                    await self._trigger_event('session_error', session_id)
                    del self.active_processes[session_id]
                    return
                
                # Update stats
                process_info.cpu_percent = process.cpu_percent()
                process_info.memory_mb = process.memory_info().rss / 1024 / 1024
                process_info.last_heartbeat = datetime.utcnow()
                
            except psutil.NoSuchProcess:
                # Process no longer exists
                process_info.status = "stopped"
                del self.active_processes[session_id]
                await self._trigger_event('session_error', session_id)
                
        except Exception as e:
            self.logger.error(f"Error updating stats for session {session_id}: {e}")
    
    def add_event_callback(self, event_type: str, callback: Callable):
        """Add an event callback"""
        if event_type in self.event_callbacks:
            self.event_callbacks[event_type].append(callback)
    
    def remove_event_callback(self, event_type: str, callback: Callable):
        """Remove an event callback"""
        if event_type in self.event_callbacks and callback in self.event_callbacks[event_type]:
            self.event_callbacks[event_type].remove(callback)
    
    async def _trigger_event(self, event_type: str, session_id: str):
        """Trigger event callbacks"""
        for callback in self.event_callbacks.get(event_type, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(session_id)
                else:
                    callback(session_id)
            except Exception as e:
                self.logger.error(f"Error in event callback {event_type}: {e}")
    
    async def get_system_stats(self) -> Dict:
        """Get overall system statistics"""
        return {
            "active_sessions": len(self.active_processes),
            "max_concurrent": self.max_concurrent,
            "available_slots": self.max_concurrent - len(self.active_processes),
            "total_cpu_percent": sum(p.cpu_percent for p in self.active_processes.values()),
            "total_memory_mb": sum(p.memory_mb for p in self.active_processes.values()),
            "system_cpu_percent": psutil.cpu_percent(),
            "system_memory_percent": psutil.virtual_memory().percent
        }
    
    async def shutdown(self):
        """Shutdown the multi-session manager"""
        self.logger.info("Shutting down MultiSessionManager...")
        
        # Stop monitoring
        await self.stop_monitoring()
        await self.process_monitor.stop_monitoring()
        
        # Stop all sessions
        if self.active_processes:
            session_ids = list(self.active_processes.keys())
            await self.stop_sessions(session_ids, force=True)
        
        # Shutdown process pool
        if self.process_pool:
            self.process_pool.shutdown(wait=True)
        
        self.logger.info("MultiSessionManager shutdown complete")


def _run_session_process(session_id: str, db_path: str, api_key: str, api_secret: str):
    """Function to run in separate process"""
    import asyncio
    from data.persistence import DatabaseManager
    from core.strategy_engine import StrategyEngine
    from core.session_manager import SessionManager
    
    async def run():
        try:
            # Initialize components in the new process
            db_manager = DatabaseManager(db_path)
            session_manager = SessionManager(db_manager)
            strategy_engine = StrategyEngine(db_manager)
            
            await session_manager.initialize()
            
            # Get session
            session = await session_manager.get_session(session_id)
            if not session:
                print(f"Session {session_id} not found")
                return
            
            # Start strategy
            success = await strategy_engine.start_strategy(session, api_key, api_secret)
            if not success:
                print(f"Failed to start strategy for session {session_id}")
                return
            
            print(f"Session {session_id} started successfully")
            
            # Keep running until stopped
            while strategy_engine.is_strategy_running(session_id):
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"Error in session process {session_id}: {e}")
        finally:
            try:
                await strategy_engine.shutdown()
            except:
                pass
    
    # Run the session
    asyncio.run(run())