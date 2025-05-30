import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import json

from data.models import Trade, Session, PerformanceMetrics
from data.persistence import DatabaseManager


@dataclass
class PerformanceReport:
    session_id: str
    session_name: str
    period_start: datetime
    period_end: datetime
    total_pnl: float
    total_return_pct: float
    win_rate: float
    profit_factor: float
    sharpe_ratio: float
    max_drawdown: float
    max_drawdown_duration_hours: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_win: float
    avg_loss: float
    largest_win: float
    largest_loss: float
    consecutive_wins: int
    consecutive_losses: int
    avg_trade_duration_minutes: float
    daily_returns: List[float]
    monthly_returns: List[float]
    trades_per_day: float
    best_day: float
    worst_day: float
    volatility: float
    calmar_ratio: float
    sortino_ratio: float


class PerformanceAnalytics:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.risk_free_rate = 0.02  # 2% annual risk-free rate
    
    async def generate_performance_report(self, session_id: str, 
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None) -> Optional[PerformanceReport]:
        """Generate comprehensive performance report for a session"""
        try:
            # Get session info
            session = await self.db_manager.load_session(session_id)
            if not session:
                return None
            
            # Get trades
            trades = await self.db_manager.get_session_trades(session_id)
            if not trades:
                return None
            
            # Filter trades by date range
            if start_date:
                trades = [t for t in trades if t.timestamp >= start_date]
            if end_date:
                trades = [t for t in trades if t.timestamp <= end_date]
            
            if not trades:
                return None
            
            # Set period bounds
            period_start = start_date or min(t.timestamp for t in trades)
            period_end = end_date or max(t.timestamp for t in trades)
            
            # Calculate basic metrics
            filled_trades = [t for t in trades if t.status == 'filled' and t.pnl is not None]
            total_pnl = sum(t.pnl for t in filled_trades)
            winning_trades = [t for t in filled_trades if t.pnl > 0]
            losing_trades = [t for t in filled_trades if t.pnl < 0]
            
            # Create DataFrame for analysis
            df = self._create_trades_dataframe(filled_trades)
            
            # Calculate metrics
            win_rate = len(winning_trades) / len(filled_trades) if filled_trades else 0
            avg_win = np.mean([t.pnl for t in winning_trades]) if winning_trades else 0
            avg_loss = np.mean([t.pnl for t in losing_trades]) if losing_trades else 0
            profit_factor = abs(sum(t.pnl for t in winning_trades) / sum(t.pnl for t in losing_trades)) if losing_trades else float('inf')
            
            # Calculate returns and risk metrics
            daily_returns = self._calculate_daily_returns(df, period_start, period_end)
            monthly_returns = self._calculate_monthly_returns(daily_returns)
            
            sharpe_ratio = self._calculate_sharpe_ratio(daily_returns)
            max_drawdown, dd_duration = self._calculate_max_drawdown(daily_returns)
            volatility = np.std(daily_returns) * np.sqrt(252) if daily_returns else 0
            sortino_ratio = self._calculate_sortino_ratio(daily_returns)
            calmar_ratio = (np.mean(daily_returns) * 252) / abs(max_drawdown) if max_drawdown != 0 else 0
            
            # Calculate consecutive wins/losses
            consecutive_wins, consecutive_losses = self._calculate_consecutive_trades(filled_trades)
            
            # Calculate trade duration
            avg_trade_duration = self._calculate_avg_trade_duration(filled_trades)
            
            # Period metrics
            period_days = (period_end - period_start).days
            trades_per_day = len(filled_trades) / period_days if period_days > 0 else 0
            
            # Assuming initial balance (this should ideally come from session config)
            initial_balance = 10000  # Default, should be configurable
            total_return_pct = (total_pnl / initial_balance) * 100
            
            best_day = max(daily_returns) if daily_returns else 0
            worst_day = min(daily_returns) if daily_returns else 0
            
            return PerformanceReport(
                session_id=session_id,
                session_name=session.name,
                period_start=period_start,
                period_end=period_end,
                total_pnl=total_pnl,
                total_return_pct=total_return_pct,
                win_rate=win_rate,
                profit_factor=profit_factor,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown,
                max_drawdown_duration_hours=dd_duration,
                total_trades=len(filled_trades),
                winning_trades=len(winning_trades),
                losing_trades=len(losing_trades),
                avg_win=avg_win,
                avg_loss=avg_loss,
                largest_win=max((t.pnl for t in winning_trades), default=0),
                largest_loss=min((t.pnl for t in losing_trades), default=0),
                consecutive_wins=consecutive_wins,
                consecutive_losses=consecutive_losses,
                avg_trade_duration_minutes=avg_trade_duration,
                daily_returns=daily_returns,
                monthly_returns=monthly_returns,
                trades_per_day=trades_per_day,
                best_day=best_day,
                worst_day=worst_day,
                volatility=volatility,
                calmar_ratio=calmar_ratio,
                sortino_ratio=sortino_ratio
            )
            
        except Exception as e:
            print(f"Error generating performance report: {e}")
            return None
    
    def _create_trades_dataframe(self, trades: List[Trade]) -> pd.DataFrame:
        """Create pandas DataFrame from trades"""
        data = []
        for trade in trades:
            data.append({
                'timestamp': trade.timestamp,
                'symbol': trade.symbol,
                'side': trade.side,
                'amount': trade.amount,
                'price': trade.price,
                'pnl': trade.pnl or 0,
                'trade_id': trade.id
            })
        
        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
        
        return df
    
    def _calculate_daily_returns(self, df: pd.DataFrame, start_date: datetime, end_date: datetime) -> List[float]:
        """Calculate daily returns from trades"""
        if df.empty:
            return []
        
        # Create date range
        date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
        
        # Group trades by date and sum PnL
        df['date'] = df['timestamp'].dt.date
        daily_pnl = df.groupby('date')['pnl'].sum()
        
        # Create complete series with zeros for days without trades
        daily_returns = []
        for date in date_range:
            daily_return = daily_pnl.get(date.date(), 0.0)
            daily_returns.append(daily_return)
        
        return daily_returns
    
    def _calculate_monthly_returns(self, daily_returns: List[float]) -> List[float]:
        """Calculate monthly returns from daily returns"""
        if not daily_returns:
            return []
        
        # Group by month and sum
        df = pd.DataFrame({'returns': daily_returns})
        df['date'] = pd.date_range(start=datetime.now().date() - timedelta(days=len(daily_returns)-1), 
                                  periods=len(daily_returns), freq='D')
        df['month'] = df['date'].dt.to_period('M')
        
        monthly_returns = df.groupby('month')['returns'].sum().tolist()
        return monthly_returns
    
    def _calculate_sharpe_ratio(self, daily_returns: List[float]) -> float:
        """Calculate Sharpe ratio"""
        if not daily_returns or len(daily_returns) < 2:
            return 0.0
        
        returns_array = np.array(daily_returns)
        mean_return = np.mean(returns_array)
        std_return = np.std(returns_array)
        
        if std_return == 0:
            return 0.0
        
        # Annualized Sharpe ratio
        daily_risk_free = self.risk_free_rate / 252
        sharpe = (mean_return - daily_risk_free) / std_return * np.sqrt(252)
        
        return sharpe
    
    def _calculate_sortino_ratio(self, daily_returns: List[float]) -> float:
        """Calculate Sortino ratio (using downside deviation)"""
        if not daily_returns or len(daily_returns) < 2:
            return 0.0
        
        returns_array = np.array(daily_returns)
        mean_return = np.mean(returns_array)
        
        # Calculate downside deviation
        downside_returns = returns_array[returns_array < 0]
        if len(downside_returns) == 0:
            return float('inf') if mean_return > 0 else 0.0
        
        downside_deviation = np.std(downside_returns)
        
        if downside_deviation == 0:
            return 0.0
        
        # Annualized Sortino ratio
        daily_risk_free = self.risk_free_rate / 252
        sortino = (mean_return - daily_risk_free) / downside_deviation * np.sqrt(252)
        
        return sortino
    
    def _calculate_max_drawdown(self, daily_returns: List[float]) -> Tuple[float, float]:
        """Calculate maximum drawdown and its duration"""
        if not daily_returns:
            return 0.0, 0.0
        
        # Calculate cumulative returns
        cumulative = np.cumsum(daily_returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = cumulative - running_max
        
        max_drawdown = np.min(drawdown)
        
        # Calculate drawdown duration
        max_dd_duration = 0
        current_dd_duration = 0
        
        for dd in drawdown:
            if dd < 0:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)
            else:
                current_dd_duration = 0
        
        # Convert to hours (assuming daily data)
        max_dd_duration_hours = max_dd_duration * 24
        
        return max_drawdown, max_dd_duration_hours
    
    def _calculate_consecutive_trades(self, trades: List[Trade]) -> Tuple[int, int]:
        """Calculate maximum consecutive wins and losses"""
        if not trades:
            return 0, 0
        
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for trade in sorted(trades, key=lambda t: t.timestamp):
            if trade.pnl and trade.pnl > 0:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            elif trade.pnl and trade.pnl < 0:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
        
        return max_consecutive_wins, max_consecutive_losses
    
    def _calculate_avg_trade_duration(self, trades: List[Trade]) -> float:
        """Calculate average trade duration in minutes"""
        # This is simplified - in a real implementation, you'd track entry/exit times
        # For now, we'll estimate based on time between trades
        if len(trades) < 2:
            return 0.0
        
        sorted_trades = sorted(trades, key=lambda t: t.timestamp)
        total_duration = 0
        
        for i in range(1, len(sorted_trades)):
            duration = (sorted_trades[i].timestamp - sorted_trades[i-1].timestamp).total_seconds() / 60
            total_duration += duration
        
        return total_duration / (len(sorted_trades) - 1)
    
    async def compare_sessions(self, session_ids: List[str], 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Compare performance across multiple sessions"""
        reports = {}
        
        for session_id in session_ids:
            report = await self.generate_performance_report(session_id, start_date, end_date)
            if report:
                reports[session_id] = report
        
        if not reports:
            return {}
        
        # Create comparison data
        comparison = {
            "sessions": {},
            "rankings": {
                "by_total_pnl": [],
                "by_sharpe_ratio": [],
                "by_win_rate": [],
                "by_max_drawdown": []
            },
            "summary": {
                "best_performer": None,
                "most_consistent": None,
                "highest_win_rate": None,
                "lowest_drawdown": None
            }
        }
        
        # Add session data
        for session_id, report in reports.items():
            comparison["sessions"][session_id] = {
                "name": report.session_name,
                "total_pnl": report.total_pnl,
                "total_return_pct": report.total_return_pct,
                "sharpe_ratio": report.sharpe_ratio,
                "win_rate": report.win_rate,
                "max_drawdown": report.max_drawdown,
                "total_trades": report.total_trades,
                "profit_factor": report.profit_factor
            }
        
        # Create rankings
        comparison["rankings"]["by_total_pnl"] = sorted(
            reports.items(), key=lambda x: x[1].total_pnl, reverse=True
        )
        comparison["rankings"]["by_sharpe_ratio"] = sorted(
            reports.items(), key=lambda x: x[1].sharpe_ratio, reverse=True
        )
        comparison["rankings"]["by_win_rate"] = sorted(
            reports.items(), key=lambda x: x[1].win_rate, reverse=True
        )
        comparison["rankings"]["by_max_drawdown"] = sorted(
            reports.items(), key=lambda x: x[1].max_drawdown
        )
        
        # Set summary
        if comparison["rankings"]["by_total_pnl"]:
            comparison["summary"]["best_performer"] = comparison["rankings"]["by_total_pnl"][0][0]
        if comparison["rankings"]["by_sharpe_ratio"]:
            comparison["summary"]["most_consistent"] = comparison["rankings"]["by_sharpe_ratio"][0][0]
        if comparison["rankings"]["by_win_rate"]:
            comparison["summary"]["highest_win_rate"] = comparison["rankings"]["by_win_rate"][0][0]
        if comparison["rankings"]["by_max_drawdown"]:
            comparison["summary"]["lowest_drawdown"] = comparison["rankings"]["by_max_drawdown"][0][0]
        
        return comparison
    
    def export_report_to_json(self, report: PerformanceReport) -> str:
        """Export performance report to JSON"""
        report_dict = {
            "session_id": report.session_id,
            "session_name": report.session_name,
            "period_start": report.period_start.isoformat(),
            "period_end": report.period_end.isoformat(),
            "total_pnl": report.total_pnl,
            "total_return_pct": report.total_return_pct,
            "win_rate": report.win_rate,
            "profit_factor": report.profit_factor,
            "sharpe_ratio": report.sharpe_ratio,
            "max_drawdown": report.max_drawdown,
            "max_drawdown_duration_hours": report.max_drawdown_duration_hours,
            "total_trades": report.total_trades,
            "winning_trades": report.winning_trades,
            "losing_trades": report.losing_trades,
            "avg_win": report.avg_win,
            "avg_loss": report.avg_loss,
            "largest_win": report.largest_win,
            "largest_loss": report.largest_loss,
            "consecutive_wins": report.consecutive_wins,
            "consecutive_losses": report.consecutive_losses,
            "avg_trade_duration_minutes": report.avg_trade_duration_minutes,
            "trades_per_day": report.trades_per_day,
            "best_day": report.best_day,
            "worst_day": report.worst_day,
            "volatility": report.volatility,
            "calmar_ratio": report.calmar_ratio,
            "sortino_ratio": report.sortino_ratio,
            "daily_returns": report.daily_returns,
            "monthly_returns": report.monthly_returns
        }
        
        return json.dumps(report_dict, indent=2)
    
    async def get_portfolio_analytics(self, session_ids: List[str]) -> Dict[str, Any]:
        """Get analytics for a portfolio of sessions"""
        try:
            # Get all sessions and their trades
            all_trades = []
            sessions_info = {}
            
            for session_id in session_ids:
                session = await self.db_manager.load_session(session_id)
                trades = await self.db_manager.get_session_trades(session_id)
                
                if session and trades:
                    sessions_info[session_id] = session.name
                    all_trades.extend(trades)
            
            if not all_trades:
                return {}
            
            # Calculate portfolio metrics
            filled_trades = [t for t in all_trades if t.status == 'filled' and t.pnl is not None]
            total_pnl = sum(t.pnl for t in filled_trades)
            
            # Daily portfolio returns
            df = self._create_trades_dataframe(filled_trades)
            if not df.empty:
                start_date = min(t.timestamp for t in filled_trades)
                end_date = max(t.timestamp for t in filled_trades)
                daily_returns = self._calculate_daily_returns(df, start_date, end_date)
            else:
                daily_returns = []
            
            # Calculate portfolio metrics
            sharpe_ratio = self._calculate_sharpe_ratio(daily_returns)
            max_drawdown, _ = self._calculate_max_drawdown(daily_returns)
            volatility = np.std(daily_returns) * np.sqrt(252) if daily_returns else 0
            
            return {
                "sessions": sessions_info,
                "total_sessions": len(session_ids),
                "total_trades": len(filled_trades),
                "total_pnl": total_pnl,
                "portfolio_sharpe": sharpe_ratio,
                "portfolio_volatility": volatility,
                "portfolio_max_drawdown": max_drawdown,
                "daily_returns": daily_returns,
                "period": {
                    "start": min(t.timestamp for t in filled_trades).isoformat() if filled_trades else None,
                    "end": max(t.timestamp for t in filled_trades).isoformat() if filled_trades else None
                }
            }
            
        except Exception as e:
            print(f"Error calculating portfolio analytics: {e}")
            return {}