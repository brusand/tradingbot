import asyncio
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import copy

from data.models import Session, SessionStatus, Trade, PerformanceMetrics
from data.persistence import DatabaseManager


@dataclass
class SessionSnapshot:
    timestamp: datetime
    session_state: Dict[str, Any]
    performance_metrics: PerformanceMetrics
    active_positions: Dict[str, float]
    recent_trades: List[Trade]
    market_data: Dict[str, Any]


@dataclass
class ReplayEvent:
    timestamp: datetime
    event_type: str  # trade, state_change, market_data, alert
    data: Dict[str, Any]
    session_id: str


class SessionReplayManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.snapshots: Dict[str, List[SessionSnapshot]] = {}  # session_id -> snapshots
        self.replay_events: Dict[str, List[ReplayEvent]] = {}  # session_id -> events
        self.snapshot_interval = 300  # seconds (5 minutes)
        
    async def create_snapshot(self, session_id: str, session: Session, additional_data: Dict = None):
        """Create a snapshot of the current session state"""
        try:
            # Get recent trades
            trades = await self.db_manager.get_session_trades(session_id)
            recent_trades = [t for t in trades if (datetime.utcnow() - t.timestamp).seconds < 3600]  # Last hour
            
            # Get current positions from session state
            active_positions = session.state.get("positions", {})
            
            # Create snapshot
            snapshot = SessionSnapshot(
                timestamp=datetime.utcnow(),
                session_state=copy.deepcopy(session.state),
                performance_metrics=copy.deepcopy(session.performance_metrics),
                active_positions=copy.deepcopy(active_positions),
                recent_trades=recent_trades[-10:],  # Last 10 trades
                market_data=additional_data.get("market_data", {}) if additional_data else {}
            )
            
            # Store snapshot
            if session_id not in self.snapshots:
                self.snapshots[session_id] = []
            
            self.snapshots[session_id].append(snapshot)
            
            # Keep only last 100 snapshots per session
            if len(self.snapshots[session_id]) > 100:
                self.snapshots[session_id] = self.snapshots[session_id][-100:]
            
            return snapshot
            
        except Exception as e:
            print(f"Error creating snapshot for session {session_id}: {e}")
            return None
    
    async def record_event(self, session_id: str, event_type: str, data: Dict[str, Any]):
        """Record an event for replay purposes"""
        try:
            event = ReplayEvent(
                timestamp=datetime.utcnow(),
                event_type=event_type,
                data=copy.deepcopy(data),
                session_id=session_id
            )
            
            if session_id not in self.replay_events:
                self.replay_events[session_id] = []
            
            self.replay_events[session_id].append(event)
            
            # Keep only last 1000 events per session
            if len(self.replay_events[session_id]) > 1000:
                self.replay_events[session_id] = self.replay_events[session_id][-1000:]
                
        except Exception as e:
            print(f"Error recording event for session {session_id}: {e}")
    
    def get_snapshots(self, session_id: str, start_time: Optional[datetime] = None, 
                      end_time: Optional[datetime] = None) -> List[SessionSnapshot]:
        """Get snapshots for a session within a time range"""
        snapshots = self.snapshots.get(session_id, [])
        
        if start_time:
            snapshots = [s for s in snapshots if s.timestamp >= start_time]
        if end_time:
            snapshots = [s for s in snapshots if s.timestamp <= end_time]
            
        return sorted(snapshots, key=lambda s: s.timestamp)
    
    def get_events(self, session_id: str, start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None, event_type: Optional[str] = None) -> List[ReplayEvent]:
        """Get events for a session with optional filtering"""
        events = self.replay_events.get(session_id, [])
        
        if start_time:
            events = [e for e in events if e.timestamp >= start_time]
        if end_time:
            events = [e for e in events if e.timestamp <= end_time]
        if event_type:
            events = [e for e in events if e.event_type == event_type]
            
        return sorted(events, key=lambda e: e.timestamp)
    
    async def replay_session(self, session_id: str, start_time: datetime, 
                            end_time: Optional[datetime] = None, speed_multiplier: float = 1.0):
        """Replay a session's events in chronological order"""
        if end_time is None:
            end_time = datetime.utcnow()
            
        # Get all events in the time range
        events = self.get_events(session_id, start_time, end_time)
        
        if not events:
            print(f"No events found for session {session_id} in the specified time range")
            return
        
        print(f"Replaying {len(events)} events for session {session_id}")
        print(f"Time range: {start_time} to {end_time}")
        print(f"Speed multiplier: {speed_multiplier}x")
        
        # Replay events
        last_timestamp = start_time
        
        for i, event in enumerate(events):
            # Calculate delay based on real time difference
            if i > 0:
                time_diff = (event.timestamp - last_timestamp).total_seconds()
                delay = time_diff / speed_multiplier
                
                if delay > 0.1:  # Minimum delay of 100ms
                    await asyncio.sleep(min(delay, 10))  # Maximum delay of 10 seconds
            
            # Process event
            await self._process_replay_event(event)
            last_timestamp = event.timestamp
            
            if i % 10 == 0:  # Progress update every 10 events
                progress = (i + 1) / len(events) * 100
                print(f"Replay progress: {progress:.1f}% ({i + 1}/{len(events)})")
        
        print("Replay completed")
    
    async def _process_replay_event(self, event: ReplayEvent):
        """Process a single replay event"""
        print(f"[{event.timestamp}] {event.event_type}: {event.data}")
        
        # Here you could implement actual replay logic
        # For now, we just print the events
        # In a full implementation, you might:
        # - Update a virtual session state
        # - Trigger strategy recalculations
        # - Update UI components
        # - Recalculate performance metrics
    
    async def duplicate_session_with_state(self, original_session_id: str, new_name: str,
                                         snapshot_time: Optional[datetime] = None) -> Optional[Session]:
        """Duplicate a session with state from a specific point in time"""
        try:
            # Get original session
            original_session = await self.db_manager.load_session(original_session_id)
            if not original_session:
                print(f"Original session {original_session_id} not found")
                return None
            
            # Find the snapshot closest to the requested time
            if snapshot_time:
                snapshots = self.get_snapshots(original_session_id, end_time=snapshot_time)
                if snapshots:
                    target_snapshot = snapshots[-1]  # Latest snapshot before the time
                else:
                    print(f"No snapshots found before {snapshot_time}")
                    target_snapshot = None
            else:
                # Use the latest snapshot
                snapshots = self.get_snapshots(original_session_id)
                target_snapshot = snapshots[-1] if snapshots else None
            
            # Create new session
            from core.session_manager import SessionManager
            session_manager = SessionManager(self.db_manager)
            await session_manager.initialize()
            
            new_session = await session_manager.create_session(
                name=new_name,
                mode=original_session.mode,
                strategy=original_session.strategy
            )
            
            # Apply state from snapshot if available
            if target_snapshot:
                new_session.state = target_snapshot.session_state
                new_session.performance_metrics = target_snapshot.performance_metrics
                new_session.updated_at = datetime.utcnow()
                
                # Save the updated session
                await self.db_manager.save_session(new_session)
                
                print(f"Duplicated session with state from {target_snapshot.timestamp}")
            else:
                print("Duplicated session with original state (no snapshots available)")
            
            return new_session
            
        except Exception as e:
            print(f"Error duplicating session: {e}")
            return None
    
    def export_session_history(self, session_id: str, start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Export complete session history for analysis"""
        snapshots = self.get_snapshots(session_id, start_time, end_time)
        events = self.get_events(session_id, start_time, end_time)
        
        return {
            "session_id": session_id,
            "export_timestamp": datetime.utcnow().isoformat(),
            "time_range": {
                "start": start_time.isoformat() if start_time else None,
                "end": end_time.isoformat() if end_time else None
            },
            "snapshots": [
                {
                    "timestamp": s.timestamp.isoformat(),
                    "session_state": s.session_state,
                    "performance_metrics": asdict(s.performance_metrics),
                    "active_positions": s.active_positions,
                    "recent_trades": [
                        {
                            "id": t.id,
                            "symbol": t.symbol,
                            "side": t.side,
                            "amount": t.amount,
                            "price": t.price,
                            "timestamp": t.timestamp.isoformat(),
                            "status": t.status,
                            "pnl": t.pnl
                        }
                        for t in s.recent_trades
                    ],
                    "market_data": s.market_data
                }
                for s in snapshots
            ],
            "events": [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "event_type": e.event_type,
                    "data": e.data
                }
                for e in events
            ]
        }
    
    def import_session_history(self, history_data: Dict[str, Any]) -> bool:
        """Import session history from exported data"""
        try:
            session_id = history_data["session_id"]
            
            # Import snapshots
            snapshots = []
            for snapshot_data in history_data["snapshots"]:
                snapshot = SessionSnapshot(
                    timestamp=datetime.fromisoformat(snapshot_data["timestamp"]),
                    session_state=snapshot_data["session_state"],
                    performance_metrics=PerformanceMetrics(**snapshot_data["performance_metrics"]),
                    active_positions=snapshot_data["active_positions"],
                    recent_trades=[
                        Trade(
                            id=t["id"],
                            session_id=session_id,
                            symbol=t["symbol"],
                            side=t["side"],
                            amount=t["amount"],
                            price=t["price"],
                            timestamp=datetime.fromisoformat(t["timestamp"]),
                            status=t["status"],
                            pnl=t["pnl"]
                        )
                        for t in snapshot_data["recent_trades"]
                    ],
                    market_data=snapshot_data["market_data"]
                )
                snapshots.append(snapshot)
            
            self.snapshots[session_id] = snapshots
            
            # Import events
            events = []
            for event_data in history_data["events"]:
                event = ReplayEvent(
                    timestamp=datetime.fromisoformat(event_data["timestamp"]),
                    event_type=event_data["event_type"],
                    data=event_data["data"],
                    session_id=session_id
                )
                events.append(event)
            
            self.replay_events[session_id] = events
            
            print(f"Imported {len(snapshots)} snapshots and {len(events)} events for session {session_id}")
            return True
            
        except Exception as e:
            print(f"Error importing session history: {e}")
            return False
    
    def get_session_timeline(self, session_id: str) -> Dict[str, Any]:
        """Get a timeline view of session activity"""
        events = self.get_events(session_id)
        snapshots = self.get_snapshots(session_id)
        
        # Combine events and snapshots into a timeline
        timeline_items = []
        
        for snapshot in snapshots:
            timeline_items.append({
                "timestamp": snapshot.timestamp.isoformat(),
                "type": "snapshot",
                "data": {
                    "performance": asdict(snapshot.performance_metrics),
                    "positions": snapshot.active_positions,
                    "trades_count": len(snapshot.recent_trades)
                }
            })
        
        for event in events:
            timeline_items.append({
                "timestamp": event.timestamp.isoformat(),
                "type": "event",
                "subtype": event.event_type,
                "data": event.data
            })
        
        # Sort by timestamp
        timeline_items.sort(key=lambda x: x["timestamp"])
        
        return {
            "session_id": session_id,
            "timeline": timeline_items,
            "summary": {
                "total_snapshots": len(snapshots),
                "total_events": len(events),
                "event_types": list(set(e.event_type for e in events)),
                "time_range": {
                    "start": timeline_items[0]["timestamp"] if timeline_items else None,
                    "end": timeline_items[-1]["timestamp"] if timeline_items else None
                }
            }
        }
    
    async def cleanup_old_data(self, max_age_days: int = 30):
        """Clean up old snapshots and events"""
        cutoff_time = datetime.utcnow() - timedelta(days=max_age_days)
        
        # Clean snapshots
        for session_id in list(self.snapshots.keys()):
            original_count = len(self.snapshots[session_id])
            self.snapshots[session_id] = [
                s for s in self.snapshots[session_id] 
                if s.timestamp > cutoff_time
            ]
            cleaned_count = original_count - len(self.snapshots[session_id])
            if cleaned_count > 0:
                print(f"Cleaned {cleaned_count} old snapshots for session {session_id}")
        
        # Clean events
        for session_id in list(self.replay_events.keys()):
            original_count = len(self.replay_events[session_id])
            self.replay_events[session_id] = [
                e for e in self.replay_events[session_id] 
                if e.timestamp > cutoff_time
            ]
            cleaned_count = original_count - len(self.replay_events[session_id])
            if cleaned_count > 0:
                print(f"Cleaned {cleaned_count} old events for session {session_id}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored replay data"""
        total_snapshots = sum(len(snapshots) for snapshots in self.snapshots.values())
        total_events = sum(len(events) for events in self.replay_events.values())
        
        return {
            "sessions_with_snapshots": len(self.snapshots),
            "sessions_with_events": len(self.replay_events),
            "total_snapshots": total_snapshots,
            "total_events": total_events,
            "average_snapshots_per_session": total_snapshots / len(self.snapshots) if self.snapshots else 0,
            "average_events_per_session": total_events / len(self.replay_events) if self.replay_events else 0
        }