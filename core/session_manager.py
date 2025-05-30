import asyncio
from typing import List, Optional, Dict
from datetime import datetime, UTC
from data.models import Session, SessionMode, SessionStatus, StrategyConfig, RiskConfig
from data.persistence import DatabaseManager


class SessionManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.active_sessions: Dict[str, Session] = {}
        
    async def initialize(self):
        await self.db_manager.init_db()
        
    async def create_session(
        self, 
        name: str, 
        mode: SessionMode, 
        strategy: StrategyConfig, 
        risk_params: RiskConfig
    ) -> Session:
        session = Session.create(name, mode, strategy, risk_params)
        await self.db_manager.save_session(session)
        return session
    
    async def start_session(self, session_id: str) -> bool:
        session = await self.get_session(session_id)
        if not session:
            return False
            
        if session.status != SessionStatus.CREATED and session.status != SessionStatus.PAUSED:
            return False
            
        session.status = SessionStatus.RUNNING
        session.updated_at = datetime.now(UTC)
        await self.db_manager.save_session(session)
        
        self.active_sessions[session_id] = session
        return True
    
    async def pause_session(self, session_id: str) -> bool:
        session = await self.get_session(session_id)
        if not session or session.status != SessionStatus.RUNNING:
            return False
            
        session.status = SessionStatus.PAUSED
        session.updated_at = datetime.now(UTC)
        await self.db_manager.save_session(session)
        
        if session_id in self.active_sessions:
            del self.active_sessions[session_id]
        return True
    
    async def stop_session(self, session_id: str) -> bool:
        session = await self.get_session(session_id)
        if not session:
            return False
            
        session.status = SessionStatus.STOPPED
        session.updated_at = datetime.now(UTC)
        await self.db_manager.save_session(session)
        
        if session_id in self.active_sessions:
            del self.active_sessions[session_id]
        return True
    
    async def get_session(self, session_id: str) -> Optional[Session]:
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]
        return await self.db_manager.load_session(session_id)
    
    async def list_sessions(self) -> List[Session]:
        return await self.db_manager.list_sessions()
    
    async def duplicate_session(self, session_id: str, new_name: str) -> Optional[Session]:
        original = await self.get_session(session_id)
        if not original:
            return None
            
        new_session = Session.create(
            name=new_name,
            mode=original.mode,
            strategy=original.strategy,
            risk_params=original.risk_params
        )
        
        await self.db_manager.save_session(new_session)
        return new_session
    
    async def delete_session(self, session_id: str) -> bool:
        session = await self.get_session(session_id)
        if not session:
            return False
            
        if session.status == SessionStatus.RUNNING:
            await self.stop_session(session_id)
            
        await self.db_manager.delete_session(session_id)
        
        if session_id in self.active_sessions:
            del self.active_sessions[session_id]
        return True
    
    async def update_session_state(self, session_id: str, state_update: Dict) -> bool:
        session = await self.get_session(session_id)
        if not session:
            return False
            
        session.state.update(state_update)
        session.updated_at = datetime.now(UTC)
        await self.db_manager.save_session(session)
        return True
    
    def get_active_sessions(self) -> List[Session]:
        return list(self.active_sessions.values())
    
    async def get_sessions_by_profitability(self) -> List[Session]:
        sessions = await self.list_sessions()
        return sorted(sessions, key=lambda s: s.performance_metrics.total_pnl, reverse=True)