import asyncio
import psutil
import signal
import os
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import json

from data.models import SessionStatus
from data.persistence import DatabaseManager


@dataclass
class ProcessHealth:
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    threads_count: int
    open_files: int
    connections: int
    last_check: datetime
    is_responsive: bool
    error_count: int


@dataclass
class ProcessAlert:
    session_id: str
    alert_type: str  # cpu_high, memory_high, not_responsive, crashed
    severity: str    # low, medium, high, critical
    message: str
    timestamp: datetime
    resolved: bool = False


class ProcessMonitor:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.monitored_processes: Dict[str, psutil.Process] = {}
        self.process_health: Dict[str, ProcessHealth] = {}
        self.alerts: List[ProcessAlert] = []
        
        # Monitoring settings
        self.check_interval = 5  # seconds
        self.cpu_threshold = 80.0  # percent
        self.memory_threshold = 500.0  # MB
        self.memory_percent_threshold = 10.0  # percent of system memory
        self.max_error_count = 3
        
        # Monitoring task
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
        
        # Alert callbacks
        self.alert_callbacks: List[Callable] = []
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def add_process(self, session_id: str, process_id: int):
        """Add a process to monitoring"""
        try:
            process = psutil.Process(process_id)
            self.monitored_processes[session_id] = process
            
            # Initialize health tracking
            self.process_health[session_id] = ProcessHealth(
                cpu_percent=0.0,
                memory_mb=0.0,
                memory_percent=0.0,
                threads_count=0,
                open_files=0,
                connections=0,
                last_check=datetime.utcnow(),
                is_responsive=True,
                error_count=0
            )
            
            self.logger.info(f"Added process {process_id} for session {session_id} to monitoring")
            
        except psutil.NoSuchProcess:
            self.logger.error(f"Process {process_id} not found for session {session_id}")
        except Exception as e:
            self.logger.error(f"Error adding process {process_id} to monitoring: {e}")
    
    def remove_process(self, session_id: str):
        """Remove a process from monitoring"""
        if session_id in self.monitored_processes:
            del self.monitored_processes[session_id]
        if session_id in self.process_health:
            del self.process_health[session_id]
        
        # Clear related alerts
        self.alerts = [alert for alert in self.alerts if alert.session_id != session_id]
        
        self.logger.info(f"Removed session {session_id} from monitoring")
    
    async def start_monitoring(self):
        """Start the monitoring loop"""
        if not self.is_monitoring:
            self.is_monitoring = True
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            self.logger.info("Started process monitoring")
    
    async def stop_monitoring(self):
        """Stop the monitoring loop"""
        if self.is_monitoring:
            self.is_monitoring = False
            if self.monitoring_task:
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass
            self.logger.info("Stopped process monitoring")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_monitoring:
            try:
                await self._check_all_processes()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_all_processes(self):
        """Check health of all monitored processes"""
        for session_id, process in list(self.monitored_processes.items()):
            try:
                await self._check_process_health(session_id, process)
            except psutil.NoSuchProcess:
                await self._handle_dead_process(session_id)
            except Exception as e:
                self.logger.error(f"Error checking process for session {session_id}: {e}")
                await self._increment_error_count(session_id)
    
    async def _check_process_health(self, session_id: str, process: psutil.Process):
        """Check health of a specific process"""
        try:
            # Get process info
            cpu_percent = process.cpu_percent()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            memory_percent = process.memory_percent()
            
            # Get additional metrics
            threads_count = process.num_threads()
            open_files = len(process.open_files()) if hasattr(process, 'open_files') else 0
            connections = len(process.connections()) if hasattr(process, 'connections') else 0
            
            # Update health record
            health = self.process_health.get(session_id)
            if health:
                health.cpu_percent = cpu_percent
                health.memory_mb = memory_mb
                health.memory_percent = memory_percent
                health.threads_count = threads_count
                health.open_files = open_files
                health.connections = connections
                health.last_check = datetime.utcnow()
                health.is_responsive = True
                health.error_count = 0  # Reset on successful check
            
            # Check thresholds and create alerts
            await self._check_thresholds(session_id, health)
            
        except psutil.AccessDenied:
            self.logger.warning(f"Access denied when checking process for session {session_id}")
        except Exception as e:
            self.logger.error(f"Error getting process info for session {session_id}: {e}")
            await self._increment_error_count(session_id)
    
    async def _check_thresholds(self, session_id: str, health: ProcessHealth):
        """Check if process metrics exceed thresholds"""
        # CPU threshold check
        if health.cpu_percent > self.cpu_threshold:
            await self._create_alert(
                session_id,
                "cpu_high",
                "high",
                f"High CPU usage: {health.cpu_percent:.1f}%"
            )
        
        # Memory threshold check
        if health.memory_mb > self.memory_threshold:
            await self._create_alert(
                session_id,
                "memory_high",
                "high",
                f"High memory usage: {health.memory_mb:.1f} MB"
            )
        
        # Memory percentage threshold check
        if health.memory_percent > self.memory_percent_threshold:
            await self._create_alert(
                session_id,
                "memory_high",
                "medium",
                f"High memory percentage: {health.memory_percent:.1f}%"
            )
        
        # Too many open files check
        if health.open_files > 100:
            await self._create_alert(
                session_id,
                "resource_high",
                "medium",
                f"Too many open files: {health.open_files}"
            )
    
    async def _handle_dead_process(self, session_id: str):
        """Handle a process that no longer exists"""
        await self._create_alert(
            session_id,
            "crashed",
            "critical",
            "Process crashed or was killed"
        )
        
        # Update session status in database
        try:
            session = await self.db_manager.load_session(session_id)
            if session:
                session.status = SessionStatus.ERROR
                session.state["last_error"] = "Process crashed"
                session.updated_at = datetime.utcnow()
                await self.db_manager.save_session(session)
        except Exception as e:
            self.logger.error(f"Error updating session status for {session_id}: {e}")
        
        # Remove from monitoring
        self.remove_process(session_id)
    
    async def _increment_error_count(self, session_id: str):
        """Increment error count for a session"""
        health = self.process_health.get(session_id)
        if health:
            health.error_count += 1
            if health.error_count >= self.max_error_count:
                await self._create_alert(
                    session_id,
                    "not_responsive",
                    "high",
                    f"Process not responsive (error count: {health.error_count})"
                )
                health.is_responsive = False
    
    async def _create_alert(self, session_id: str, alert_type: str, severity: str, message: str):
        """Create a new alert"""
        # Check if similar alert already exists and is not resolved
        existing_alert = self._find_existing_alert(session_id, alert_type)
        if existing_alert and not existing_alert.resolved:
            return  # Don't create duplicate alerts
        
        alert = ProcessAlert(
            session_id=session_id,
            alert_type=alert_type,
            severity=severity,
            message=message,
            timestamp=datetime.utcnow()
        )
        
        self.alerts.append(alert)
        self.logger.warning(f"Alert created for session {session_id}: {message}")
        
        # Trigger callbacks
        for callback in self.alert_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(alert)
                else:
                    callback(alert)
            except Exception as e:
                self.logger.error(f"Error in alert callback: {e}")
    
    def _find_existing_alert(self, session_id: str, alert_type: str) -> Optional[ProcessAlert]:
        """Find existing alert of the same type for a session"""
        for alert in reversed(self.alerts):  # Check recent alerts first
            if (alert.session_id == session_id and 
                alert.alert_type == alert_type and 
                not alert.resolved):
                return alert
        return None
    
    def resolve_alert(self, session_id: str, alert_type: str):
        """Mark alerts as resolved"""
        for alert in self.alerts:
            if (alert.session_id == session_id and 
                alert.alert_type == alert_type and 
                not alert.resolved):
                alert.resolved = True
    
    def get_process_health(self, session_id: str) -> Optional[ProcessHealth]:
        """Get health information for a specific session"""
        return self.process_health.get(session_id)
    
    def get_all_health_data(self) -> Dict[str, ProcessHealth]:
        """Get health data for all monitored processes"""
        return self.process_health.copy()
    
    def get_alerts(self, session_id: Optional[str] = None, unresolved_only: bool = True) -> List[ProcessAlert]:
        """Get alerts, optionally filtered by session ID and resolution status"""
        alerts = self.alerts
        
        if session_id:
            alerts = [alert for alert in alerts if alert.session_id == session_id]
        
        if unresolved_only:
            alerts = [alert for alert in alerts if not alert.resolved]
        
        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)
    
    def add_alert_callback(self, callback: Callable):
        """Add a callback to be called when alerts are created"""
        self.alert_callbacks.append(callback)
    
    def remove_alert_callback(self, callback: Callable):
        """Remove an alert callback"""
        if callback in self.alert_callbacks:
            self.alert_callbacks.remove(callback)
    
    async def kill_process(self, session_id: str, force: bool = False) -> bool:
        """Kill a monitored process"""
        if session_id not in self.monitored_processes:
            return False
        
        try:
            process = self.monitored_processes[session_id]
            
            if force:
                process.kill()
                self.logger.info(f"Force killed process for session {session_id}")
            else:
                process.terminate()
                
                # Wait for graceful termination
                try:
                    process.wait(timeout=5)
                except psutil.TimeoutExpired:
                    # Force kill if graceful termination fails
                    process.kill()
                    self.logger.info(f"Force killed process for session {session_id} after timeout")
                else:
                    self.logger.info(f"Gracefully terminated process for session {session_id}")
            
            # Remove from monitoring
            self.remove_process(session_id)
            return True
            
        except psutil.NoSuchProcess:
            # Process already dead
            self.remove_process(session_id)
            return True
        except Exception as e:
            self.logger.error(f"Error killing process for session {session_id}: {e}")
            return False
    
    def get_system_stats(self) -> Dict:
        """Get overall system statistics"""
        total_cpu = sum(health.cpu_percent for health in self.process_health.values())
        total_memory = sum(health.memory_mb for health in self.process_health.values())
        
        return {
            "monitored_processes": len(self.monitored_processes),
            "total_cpu_percent": total_cpu,
            "total_memory_mb": total_memory,
            "system_cpu_percent": psutil.cpu_percent(),
            "system_memory_percent": psutil.virtual_memory().percent,
            "system_memory_available_gb": psutil.virtual_memory().available / 1024 / 1024 / 1024,
            "active_alerts": len(self.get_alerts(unresolved_only=True)),
            "total_alerts": len(self.alerts)
        }
    
    async def cleanup_old_alerts(self, max_age_hours: int = 24):
        """Remove old resolved alerts"""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        initial_count = len(self.alerts)
        
        self.alerts = [
            alert for alert in self.alerts
            if not (alert.resolved and alert.timestamp < cutoff_time)
        ]
        
        removed_count = initial_count - len(self.alerts)
        if removed_count > 0:
            self.logger.info(f"Cleaned up {removed_count} old alerts")
    
    def export_health_data(self) -> str:
        """Export health data as JSON"""
        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "system_stats": self.get_system_stats(),
            "process_health": {
                session_id: {
                    "cpu_percent": health.cpu_percent,
                    "memory_mb": health.memory_mb,
                    "memory_percent": health.memory_percent,
                    "threads_count": health.threads_count,
                    "open_files": health.open_files,
                    "connections": health.connections,
                    "last_check": health.last_check.isoformat(),
                    "is_responsive": health.is_responsive,
                    "error_count": health.error_count
                }
                for session_id, health in self.process_health.items()
            },
            "alerts": [
                {
                    "session_id": alert.session_id,
                    "alert_type": alert.alert_type,
                    "severity": alert.severity,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved
                }
                for alert in self.alerts
            ]
        }
        
        return json.dumps(data, indent=2)