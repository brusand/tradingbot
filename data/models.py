from dataclasses import dataclass
from datetime import datetime, UTC
from enum import Enum
from typing import Optional, Dict, Any, List
import uuid


class SessionMode(Enum):
    SANDBOX = "sandbox"
    PAPER = "paper"
    LIVE = "live"


class SessionStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class RiskConfig:
    max_position_size: float
    stop_loss_pct: float
    take_profit_pct: float
    max_daily_loss: float
    max_exposure_pct: float
    risk_ratio: float = 2.0  # Risk/Reward ratio (1:RR) - ex: 2.0 = 1:2


@dataclass
class StrategyConfig:
    name: str
    parameters: Dict[str, Any]
    timeframe: str
    pairs: List[str]
    risk_config: Optional[RiskConfig] = None  # Risk management au niveau stratégie


@dataclass
class PerformanceMetrics:
    total_pnl: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0


@dataclass
class Session:
    id: str
    name: str
    mode: SessionMode
    strategy: StrategyConfig
    risk_params: RiskConfig
    created_at: datetime
    updated_at: datetime
    status: SessionStatus
    state: Dict[str, Any]
    performance_metrics: PerformanceMetrics
    
    @classmethod
    def create(cls, name: str, mode: SessionMode, strategy: StrategyConfig, risk_params: RiskConfig):
        now = datetime.now(UTC)
        return cls(
            id=str(uuid.uuid4()),
            name=name,
            mode=mode,
            strategy=strategy,
            risk_params=risk_params,
            created_at=now,
            updated_at=now,
            status=SessionStatus.CREATED,
            state={},
            performance_metrics=PerformanceMetrics()
        )


@dataclass
class Trade:
    id: str
    session_id: str
    symbol: str
    side: str  # buy/sell
    amount: float
    price: float
    timestamp: datetime
    status: str  # pending/filled/cancelled
    pnl: Optional[float] = None