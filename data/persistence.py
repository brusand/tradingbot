import aiosqlite
import json
from datetime import datetime
from typing import List, Optional
from .models import Session, SessionMode, SessionStatus, StrategyConfig, RiskConfig, PerformanceMetrics, Trade


class DatabaseManager:
    def __init__(self, db_path: str = "trading_bot.db"):
        self.db_path = db_path
    
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    strategy_config TEXT NOT NULL,
                    risk_config TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    state TEXT NOT NULL,
                    performance_metrics TEXT NOT NULL
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL NOT NULL,
                    price REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    pnl REAL,
                    FOREIGN KEY (session_id) REFERENCES sessions (id)
                )
            """)
            
            await db.commit()
    
    async def save_session(self, session: Session):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO sessions 
                (id, name, mode, strategy_config, risk_config, created_at, updated_at, status, state, performance_metrics)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session.id,
                session.name,
                session.mode.value,
                json.dumps(session.strategy.__dict__),
                session.created_at.isoformat(),
                session.updated_at.isoformat(),
                session.status.value,
                json.dumps(session.state),
                json.dumps(session.performance_metrics.__dict__)
            ))
            await db.commit()
    
    async def load_session(self, session_id: str) -> Optional[Session]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return self._row_to_session(row)
                return None
    
    async def list_sessions(self) -> List[Session]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT * FROM sessions ORDER BY created_at DESC") as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_session(row) for row in rows]
    
    async def delete_session(self, session_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            await db.execute("DELETE FROM trades WHERE session_id = ?", (session_id,))
            await db.commit()
    
    async def save_trade(self, trade: Trade):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO trades 
                (id, session_id, symbol, side, amount, price, timestamp, status, pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.id,
                trade.session_id,
                trade.symbol,
                trade.side,
                trade.amount,
                trade.price,
                trade.timestamp.isoformat(),
                trade.status,
                trade.pnl
            ))
            await db.commit()
    
    async def get_session_trades(self, session_id: str) -> List[Trade]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT * FROM trades WHERE session_id = ? ORDER BY timestamp", (session_id,)) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_trade(row) for row in rows]
    
    def _row_to_session(self, row) -> Session:
        strategy_data = json.loads(row[3])
        state_data = json.loads(row[7])
        metrics_data = json.loads(row[8])
        
        strategy = StrategyConfig(
            name=strategy_data['name'],
            parameters=strategy_data['parameters'],
            timeframe=strategy_data['timeframe'],
            pairs=strategy_data['pairs']
        )


        performance_metrics = PerformanceMetrics(
            total_pnl=metrics_data['total_pnl'],
            win_rate=metrics_data['win_rate'],
            total_trades=metrics_data['total_trades'],
            winning_trades=metrics_data['winning_trades'],
            losing_trades=metrics_data['losing_trades'],
            max_drawdown=metrics_data['max_drawdown'],
            sharpe_ratio=metrics_data['sharpe_ratio']
        )
        
        return Session(
            id=row[0],
            name=row[1],
            mode=SessionMode(row[2]),
            strategy=strategy,
            created_at=datetime.fromisoformat(row[4]),
            updated_at=datetime.fromisoformat(row[5]),
            status=SessionStatus(row[6]),
            state=state_data,
            performance_metrics=performance_metrics
        )
    
    def _row_to_trade(self, row) -> Trade:
        return Trade(
            id=row[0],
            session_id=row[1],
            symbol=row[2],
            side=row[3],
            amount=row[4],
            price=row[5],
            timestamp=datetime.fromisoformat(row[6]),
            status=row[7],
            pnl=row[8]
        )