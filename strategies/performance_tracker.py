import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, UTC
from dataclasses import dataclass, field
from enum import Enum

class TradeStatus(Enum):
    OPEN = "open"
    CLOSED = "closed"
    CANCELLED = "cancelled"

@dataclass
class TradeRecord:
    """Enregistrement détaillé d'un trade"""
    id: str
    strategy_id: str
    symbol: str
    side: str  # "buy" ou "sell"
    entry_price: float
    entry_time: datetime
    quantity: float
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    status: TradeStatus = TradeStatus.OPEN
    pnl: float = 0.0
    pnl_pct: float = 0.0
    fees: float = 0.0
    signal_id: Optional[str] = None
    signal_strength: int = 1
    max_favorable_excursion: float = 0.0  # MFE
    max_adverse_excursion: float = 0.0    # MAE
    holding_time_seconds: float = 0.0
    tags: List[str] = field(default_factory=list)

@dataclass 
class PerformanceMetrics:
    """Métriques de performance complètes"""
    # Métriques de base
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    
    # Métriques de risque
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_duration_days: float = 0.0
    current_drawdown: float = 0.0
    current_drawdown_pct: float = 0.0
    
    # Métriques de rendement
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    profit_factor: float = 0.0
    
    # Statistiques des trades
    avg_win: float = 0.0
    avg_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    avg_trade: float = 0.0
    
    # Métriques temporelles
    avg_holding_time_hours: float = 0.0
    avg_win_time_hours: float = 0.0
    avg_loss_time_hours: float = 0.0
    
    # Métriques de consistance
    consecutive_wins: int = 0
    consecutive_losses: int = 0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    
    # Métriques avancées
    expectancy: float = 0.0  # Espérance mathématique par trade
    recovery_factor: float = 0.0  # Total PnL / Max Drawdown
    ulcer_index: float = 0.0
    var_95: float = 0.0  # Value at Risk 95%
    
    # Métadonnées
    first_trade_date: Optional[datetime] = None
    last_trade_date: Optional[datetime] = None
    total_fees: float = 0.0
    strategy_start_time: Optional[datetime] = None
    last_update: Optional[datetime] = None

class PerformanceTracker:
    """Tracker de performance pour les stratégies de trading"""
    
    def __init__(self, strategy_id: str, initial_capital: float = 10000.0):
        self.strategy_id = strategy_id
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        
        # Historique des trades
        self.trades: List[TradeRecord] = []
        self.open_trades: Dict[str, TradeRecord] = {}
        
        # Historique des valeurs du portefeuille
        self.equity_curve: List[Tuple[datetime, float]] = []
        self.daily_returns: List[float] = []
        
        # Métriques calculées
        self.metrics = PerformanceMetrics()
        self.metrics.strategy_start_time = datetime.now(UTC)
        
        # Cache pour optimiser les calculs
        self._metrics_cache_timestamp: Optional[datetime] = None
        self._cached_metrics: Optional[PerformanceMetrics] = None
        
    def add_trade_entry(self, trade_id: str, symbol: str, side: str, 
                       price: float, quantity: float, signal_id: str = None,
                       signal_strength: int = 1, fees: float = 0.0) -> TradeRecord:
        """Enregistre l'ouverture d'un trade"""
        trade = TradeRecord(
            id=trade_id,
            strategy_id=self.strategy_id,
            symbol=symbol,
            side=side,
            entry_price=price,
            entry_time=datetime.now(UTC),
            quantity=quantity,
            signal_id=signal_id,
            signal_strength=signal_strength,
            fees=fees
        )
        
        self.open_trades[trade_id] = trade
        return trade
    
    def close_trade(self, trade_id: str, exit_price: float, 
                   exit_time: datetime = None, additional_fees: float = 0.0) -> Optional[TradeRecord]:
        """Ferme un trade et calcule le PnL"""
        if trade_id not in self.open_trades:
            return None
        
        trade = self.open_trades.pop(trade_id)
        trade.exit_price = exit_price
        trade.exit_time = exit_time or datetime.now(UTC)
        trade.status = TradeStatus.CLOSED
        trade.fees += additional_fees
        
        # Calculer PnL
        if trade.side == "buy":
            trade.pnl = (exit_price - trade.entry_price) * trade.quantity - trade.fees
        else:  # sell/short
            trade.pnl = (trade.entry_price - exit_price) * trade.quantity - trade.fees
        
        trade.pnl_pct = (trade.pnl / (trade.entry_price * trade.quantity)) * 100
        
        # Calculer le temps de détention
        trade.holding_time_seconds = (trade.exit_time - trade.entry_time).total_seconds()
        
        # Mettre à jour le capital
        self.current_capital += trade.pnl
        
        # Ajouter à l'historique
        self.trades.append(trade)
        
        # Mettre à jour la courbe d'équité
        self.equity_curve.append((trade.exit_time, self.current_capital))
        
        # Invalider le cache
        self._invalidate_cache()
        
        return trade
    
    def update_open_trade_excursion(self, trade_id: str, current_price: float):
        """Met à jour les excursions favorables/défavorables d'un trade ouvert"""
        if trade_id not in self.open_trades:
            return
        
        trade = self.open_trades[trade_id]
        
        if trade.side == "buy":
            # Pour un achat: favorable = prix monte, défavorable = prix baisse
            favorable_excursion = current_price - trade.entry_price
            adverse_excursion = trade.entry_price - current_price
        else:  # sell/short
            # Pour une vente: favorable = prix baisse, défavorable = prix monte
            favorable_excursion = trade.entry_price - current_price
            adverse_excursion = current_price - trade.entry_price
        
        # Mettre à jour les maximums
        if favorable_excursion > trade.max_favorable_excursion:
            trade.max_favorable_excursion = favorable_excursion
        
        if adverse_excursion > trade.max_adverse_excursion:
            trade.max_adverse_excursion = adverse_excursion
    
    def calculate_metrics(self, force_recalculate: bool = False) -> PerformanceMetrics:
        """Calcule toutes les métriques de performance"""
        
        # Utiliser le cache si disponible et récent
        if (not force_recalculate and self._cached_metrics and 
            self._metrics_cache_timestamp and 
            (datetime.now(UTC) - self._metrics_cache_timestamp).total_seconds() < 60):
            return self._cached_metrics
        
        metrics = PerformanceMetrics()
        metrics.strategy_start_time = self.metrics.strategy_start_time
        metrics.last_update = datetime.now(UTC)
        
        if not self.trades:
            self._cached_metrics = metrics
            self._metrics_cache_timestamp = datetime.now(UTC)
            return metrics
        
        # Métriques de base
        closed_trades = [t for t in self.trades if t.status == TradeStatus.CLOSED]
        metrics.total_trades = len(closed_trades)
        
        if metrics.total_trades == 0:
            self._cached_metrics = metrics
            self._metrics_cache_timestamp = datetime.now(UTC)
            return metrics
        
        # PnL total
        total_pnl = sum(t.pnl for t in closed_trades)
        metrics.total_pnl = total_pnl
        metrics.total_pnl_pct = (total_pnl / self.initial_capital) * 100
        
        # Trades gagnants/perdants
        winning_trades = [t for t in closed_trades if t.pnl > 0]
        losing_trades = [t for t in closed_trades if t.pnl < 0]
        
        metrics.winning_trades = len(winning_trades)
        metrics.losing_trades = len(losing_trades)
        metrics.win_rate = (metrics.winning_trades / metrics.total_trades) * 100
        
        # Statistiques des trades
        if winning_trades:
            metrics.avg_win = np.mean([t.pnl for t in winning_trades])
            metrics.largest_win = max(t.pnl for t in winning_trades)
        
        if losing_trades:
            metrics.avg_loss = np.mean([t.pnl for t in losing_trades])
            metrics.largest_loss = min(t.pnl for t in losing_trades)
        
        metrics.avg_trade = total_pnl / metrics.total_trades
        
        # Métriques temporelles
        holding_times = [t.holding_time_seconds / 3600 for t in closed_trades]  # en heures
        metrics.avg_holding_time_hours = np.mean(holding_times)
        
        if winning_trades:
            win_times = [t.holding_time_seconds / 3600 for t in winning_trades]
            metrics.avg_win_time_hours = np.mean(win_times)
        
        if losing_trades:
            loss_times = [t.holding_time_seconds / 3600 for t in losing_trades]
            metrics.avg_loss_time_hours = np.mean(loss_times)
        
        # Calculs avancés
        self._calculate_drawdown_metrics(metrics)
        self._calculate_risk_metrics(metrics)
        self._calculate_consistency_metrics(metrics)
        
        # Métadonnées
        metrics.first_trade_date = min(t.entry_time for t in closed_trades)
        metrics.last_trade_date = max(t.exit_time for t in closed_trades)
        metrics.total_fees = sum(t.fees for t in closed_trades)
        
        # Cacher le résultat
        self._cached_metrics = metrics
        self._metrics_cache_timestamp = datetime.now(UTC)
        
        return metrics
    
    def _calculate_drawdown_metrics(self, metrics: PerformanceMetrics):
        """Calcule les métriques de drawdown"""
        if len(self.equity_curve) < 2:
            return
        
        # Créer une série temporelle des valeurs
        equity_values = [value for _, value in self.equity_curve]
        
        # Calculer les drawdowns
        peak = equity_values[0]
        max_dd = 0.0
        max_dd_pct = 0.0
        current_dd = 0.0
        current_dd_pct = 0.0
        
        drawdown_start = None
        max_dd_duration = 0.0
        
        for i, value in enumerate(equity_values):
            if value > peak:
                # Nouveau pic
                peak = value
                if drawdown_start:
                    # Fin du drawdown
                    duration = (self.equity_curve[i][0] - drawdown_start).total_seconds() / 86400  # jours
                    max_dd_duration = max(max_dd_duration, duration)
                    drawdown_start = None
            else:
                # En drawdown
                dd = peak - value
                dd_pct = (dd / peak) * 100 if peak > 0 else 0
                
                if dd > max_dd:
                    max_dd = dd
                    max_dd_pct = dd_pct
                
                if not drawdown_start:
                    drawdown_start = self.equity_curve[i][0]
        
        # Drawdown actuel
        current_value = equity_values[-1]
        current_peak = max(equity_values)
        
        if current_value < current_peak:
            current_dd = current_peak - current_value
            current_dd_pct = (current_dd / current_peak) * 100
        
        metrics.max_drawdown = max_dd
        metrics.max_drawdown_pct = max_dd_pct
        metrics.max_drawdown_duration_days = max_dd_duration
        metrics.current_drawdown = current_dd
        metrics.current_drawdown_pct = current_dd_pct
    
    def _calculate_risk_metrics(self, metrics: PerformanceMetrics):
        """Calcule les métriques de risque"""
        if metrics.total_trades < 2:
            return
        
        closed_trades = [t for t in self.trades if t.status == TradeStatus.CLOSED]
        returns = [t.pnl for t in closed_trades]
        
        # Sharpe Ratio (approximation sans taux sans risque)
        if len(returns) > 1:
            std_dev = np.std(returns)
            if std_dev > 0:
                metrics.sharpe_ratio = np.mean(returns) / std_dev
        
        # Sortino Ratio (uniquement écart-type des pertes)
        negative_returns = [r for r in returns if r < 0]
        if negative_returns:
            downside_deviation = np.std(negative_returns)
            if downside_deviation > 0:
                metrics.sortino_ratio = np.mean(returns) / downside_deviation
        
        # Profit Factor
        gross_profit = sum(r for r in returns if r > 0)
        gross_loss = abs(sum(r for r in returns if r < 0))
        if gross_loss > 0:
            metrics.profit_factor = gross_profit / gross_loss
        
        # Calmar Ratio
        if metrics.max_drawdown > 0:
            annualized_return = metrics.total_pnl_pct  # Approximation
            metrics.calmar_ratio = annualized_return / metrics.max_drawdown_pct
        
        # Expectancy
        win_rate_decimal = metrics.win_rate / 100
        avg_win = metrics.avg_win if metrics.avg_win > 0 else 0
        avg_loss = abs(metrics.avg_loss) if metrics.avg_loss < 0 else 0
        
        metrics.expectancy = (win_rate_decimal * avg_win) - ((1 - win_rate_decimal) * avg_loss)
        
        # Recovery Factor
        if metrics.max_drawdown > 0:
            metrics.recovery_factor = metrics.total_pnl / metrics.max_drawdown
        
        # VaR 95%
        if len(returns) > 5:
            metrics.var_95 = np.percentile(returns, 5)
    
    def _calculate_consistency_metrics(self, metrics: PerformanceMetrics):
        """Calcule les métriques de consistance"""
        closed_trades = [t for t in self.trades if t.status == TradeStatus.CLOSED]
        
        if not closed_trades:
            return
        
        # Séquences de gains/pertes consécutives
        current_wins = 0
        current_losses = 0
        max_wins = 0
        max_losses = 0
        
        for trade in closed_trades:
            if trade.pnl > 0:
                current_wins += 1
                current_losses = 0
                max_wins = max(max_wins, current_wins)
            elif trade.pnl < 0:
                current_losses += 1
                current_wins = 0
                max_losses = max(max_losses, current_losses)
        
        metrics.consecutive_wins = current_wins
        metrics.consecutive_losses = current_losses
        metrics.max_consecutive_wins = max_wins
        metrics.max_consecutive_losses = max_losses
    
    def _invalidate_cache(self):
        """Invalide le cache des métriques"""
        self._cached_metrics = None
        self._metrics_cache_timestamp = None
    
    def get_trade_history_df(self) -> pd.DataFrame:
        """Retourne l'historique des trades sous forme de DataFrame"""
        if not self.trades:
            return pd.DataFrame()
        
        data = []
        for trade in self.trades:
            data.append({
                'trade_id': trade.id,
                'symbol': trade.symbol,
                'side': trade.side,
                'entry_price': trade.entry_price,
                'exit_price': trade.exit_price,
                'quantity': trade.quantity,
                'pnl': trade.pnl,
                'pnl_pct': trade.pnl_pct,
                'entry_time': trade.entry_time,
                'exit_time': trade.exit_time,
                'holding_time_hours': trade.holding_time_seconds / 3600,
                'fees': trade.fees,
                'signal_id': trade.signal_id,
                'mfe': trade.max_favorable_excursion,
                'mae': trade.max_adverse_excursion,
                'status': trade.status.value
            })
        
        return pd.DataFrame(data)
    
    def get_equity_curve_df(self) -> pd.DataFrame:
        """Retourne la courbe d'équité sous forme de DataFrame"""
        if not self.equity_curve:
            return pd.DataFrame()
        
        data = []
        for timestamp, value in self.equity_curve:
            data.append({
                'timestamp': timestamp,
                'equity': value,
                'pnl': value - self.initial_capital,
                'pnl_pct': ((value - self.initial_capital) / self.initial_capital) * 100
            })
        
        return pd.DataFrame(data)
    
    def get_monthly_returns(self) -> pd.DataFrame:
        """Calcule les rendements mensuels"""
        equity_df = self.get_equity_curve_df()
        if equity_df.empty:
            return pd.DataFrame()
        
        equity_df['timestamp'] = pd.to_datetime(equity_df['timestamp'])
        equity_df = equity_df.set_index('timestamp')
        
        # Resampler par mois
        monthly = equity_df['equity'].resample('M').last()
        monthly_returns = monthly.pct_change() * 100
        
        return monthly_returns.to_frame('monthly_return_pct')
    
    def export_summary_report(self) -> Dict:
        """Exporte un rapport de synthèse complet"""
        metrics = self.calculate_metrics()
        
        return {
            'strategy_id': self.strategy_id,
            'summary': {
                'initial_capital': self.initial_capital,
                'current_capital': self.current_capital,
                'total_pnl': metrics.total_pnl,
                'total_pnl_pct': metrics.total_pnl_pct,
                'total_trades': metrics.total_trades,
                'win_rate': metrics.win_rate,
                'max_drawdown_pct': metrics.max_drawdown_pct
            },
            'performance_metrics': {
                'sharpe_ratio': metrics.sharpe_ratio,
                'sortino_ratio': metrics.sortino_ratio,
                'profit_factor': metrics.profit_factor,
                'expectancy': metrics.expectancy,
                'recovery_factor': metrics.recovery_factor
            },
            'trade_stats': {
                'winning_trades': metrics.winning_trades,
                'losing_trades': metrics.losing_trades,
                'avg_win': metrics.avg_win,
                'avg_loss': metrics.avg_loss,
                'largest_win': metrics.largest_win,
                'largest_loss': metrics.largest_loss,
                'avg_holding_time_hours': metrics.avg_holding_time_hours
            },
            'consistency': {
                'max_consecutive_wins': metrics.max_consecutive_wins,
                'max_consecutive_losses': metrics.max_consecutive_losses,
                'current_streak_wins': metrics.consecutive_wins,
                'current_streak_losses': metrics.consecutive_losses
            },
            'risk_metrics': {
                'max_drawdown': metrics.max_drawdown,
                'max_drawdown_pct': metrics.max_drawdown_pct,
                'current_drawdown_pct': metrics.current_drawdown_pct,
                'var_95': metrics.var_95,
                'ulcer_index': metrics.ulcer_index
            },
            'periods': {
                'strategy_start': metrics.strategy_start_time,
                'first_trade': metrics.first_trade_date,
                'last_trade': metrics.last_trade_date,
                'last_update': metrics.last_update
            },
            'open_trades_count': len(self.open_trades),
            'total_fees': metrics.total_fees
        }