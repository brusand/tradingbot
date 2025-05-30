import pytest
import pytest_asyncio
import pandas as pd
import numpy as np
from datetime import datetime, UTC, timedelta
from strategies.performance_tracker import (
    PerformanceTracker, 
    TradeRecord, 
    PerformanceMetrics,
    TradeStatus
)


class TestPerformanceTracker:
    """Tests pour le tracker de performance"""
    
    def setup_method(self):
        """Setup pour chaque test"""
        self.tracker = PerformanceTracker("test_strategy", initial_capital=10000.0)
    
    def test_initialization(self):
        """Test d'initialisation du tracker"""
        assert self.tracker.strategy_id == "test_strategy"
        assert self.tracker.initial_capital == 10000.0
        assert self.tracker.current_capital == 10000.0
        assert len(self.tracker.trades) == 0
        assert len(self.tracker.open_trades) == 0
    
    def test_add_trade_entry(self):
        """Test d'ajout d'un trade"""
        trade = self.tracker.add_trade_entry(
            trade_id="test_trade_1",
            symbol="BTCUSD",
            side="buy",
            price=50000.0,
            quantity=0.1,
            signal_id="signal_1"
        )
        
        assert trade.id == "test_trade_1"
        assert trade.symbol == "BTCUSD"
        assert trade.side == "buy"
        assert trade.entry_price == 50000.0
        assert trade.quantity == 0.1
        assert trade.status == TradeStatus.OPEN
        assert "test_trade_1" in self.tracker.open_trades
    
    def test_close_trade_profitable(self):
        """Test de fermeture d'un trade profitable"""
        # Ajouter un trade
        self.tracker.add_trade_entry(
            trade_id="test_trade_1",
            symbol="BTCUSD", 
            side="buy",
            price=50000.0,
            quantity=0.1
        )
        
        # Fermer avec profit
        closed_trade = self.tracker.close_trade("test_trade_1", 55000.0)
        
        assert closed_trade is not None
        assert closed_trade.exit_price == 55000.0
        assert closed_trade.status == TradeStatus.CLOSED
        assert closed_trade.pnl == 500.0  # (55000 - 50000) * 0.1
        assert closed_trade.pnl_pct == 10.0  # 500 / 5000 * 100
        assert self.tracker.current_capital == 10500.0
        assert "test_trade_1" not in self.tracker.open_trades
        assert len(self.tracker.trades) == 1
    
    def test_close_trade_loss(self):
        """Test de fermeture d'un trade en perte"""
        # Ajouter un trade
        self.tracker.add_trade_entry(
            trade_id="test_trade_1",
            symbol="BTCUSD",
            side="buy", 
            price=50000.0,
            quantity=0.1
        )
        
        # Fermer avec perte
        closed_trade = self.tracker.close_trade("test_trade_1", 45000.0)
        
        assert closed_trade.pnl == -500.0  # (45000 - 50000) * 0.1
        assert closed_trade.pnl_pct == -10.0
        assert self.tracker.current_capital == 9500.0
    
    def test_close_trade_short(self):
        """Test de fermeture d'un trade short"""
        # Ajouter un trade short
        self.tracker.add_trade_entry(
            trade_id="test_trade_1",
            symbol="BTCUSD",
            side="sell",
            price=50000.0,
            quantity=0.1
        )
        
        # Fermer avec profit (prix baisse)
        closed_trade = self.tracker.close_trade("test_trade_1", 45000.0)
        
        assert closed_trade.pnl == 500.0  # (50000 - 45000) * 0.1
        assert self.tracker.current_capital == 10500.0
    
    def test_multiple_trades_metrics(self):
        """Test de métriques avec plusieurs trades"""
        # Créer plusieurs trades
        trades_data = [
            ("trade_1", "buy", 50000, 55000, 0.1),  # +500
            ("trade_2", "buy", 51000, 49000, 0.1),  # -200
            ("trade_3", "sell", 52000, 50000, 0.1), # +200
            ("trade_4", "buy", 48000, 50000, 0.1),  # +200
        ]
        
        for trade_id, side, entry, exit, qty in trades_data:
            self.tracker.add_trade_entry(trade_id, "BTCUSD", side, entry, qty)
            self.tracker.close_trade(trade_id, exit)
        
        metrics = self.tracker.calculate_metrics()
        
        assert metrics.total_trades == 4
        assert metrics.total_pnl == 700.0  # 500 - 200 + 200 + 200
        assert metrics.winning_trades == 3
        assert metrics.losing_trades == 1
        assert metrics.win_rate == 75.0
        assert metrics.largest_win == 500.0
        assert metrics.largest_loss == -200.0
    
    def test_drawdown_calculation(self):
        """Test de calcul de drawdown"""
        # Simuler une série de trades avec drawdown
        trades = [
            ("trade_1", "buy", 50000, 55000, 0.1),  # +500 -> 10500
            ("trade_2", "buy", 55000, 52000, 0.1),  # -300 -> 10200  
            ("trade_3", "buy", 52000, 48000, 0.1),  # -400 -> 9800 (drawdown max)
            ("trade_4", "buy", 48000, 53000, 0.1),  # +500 -> 10300
        ]
        
        for trade_id, side, entry, exit, qty in trades:
            self.tracker.add_trade_entry(trade_id, "BTCUSD", side, entry, qty)
            self.tracker.close_trade(trade_id, exit)
        
        metrics = self.tracker.calculate_metrics()
        
        # Drawdown maximum: de 10500 à 9800 = 700
        assert metrics.max_drawdown == 700.0
        assert abs(metrics.max_drawdown_pct - 6.67) < 0.1  # 700/10500 * 100
    
    def test_excursion_tracking(self):
        """Test de suivi des excursions"""
        # Ajouter un trade
        trade = self.tracker.add_trade_entry(
            "test_trade", "BTCUSD", "buy", 50000.0, 0.1
        )
        
        # Simuler des mouvements de prix
        self.tracker.update_open_trade_excursion("test_trade", 52000)  # +2000 favorable
        self.tracker.update_open_trade_excursion("test_trade", 48000)  # -2000 adverse
        self.tracker.update_open_trade_excursion("test_trade", 54000)  # +4000 favorable
        
        assert trade.max_favorable_excursion == 4000.0
        assert trade.max_adverse_excursion == 2000.0
    
    def test_trade_history_dataframe(self):
        """Test de génération du DataFrame d'historique"""
        # Ajouter quelques trades
        self.tracker.add_trade_entry("trade_1", "BTCUSD", "buy", 50000, 0.1)
        self.tracker.close_trade("trade_1", 55000)
        
        self.tracker.add_trade_entry("trade_2", "ETHUSD", "sell", 3000, 1.0)
        self.tracker.close_trade("trade_2", 2800)
        
        df = self.tracker.get_trade_history_df()
        
        assert len(df) == 2
        assert "trade_id" in df.columns
        assert "pnl" in df.columns
        assert "symbol" in df.columns
        assert df.iloc[0]["pnl"] == 500.0
        assert df.iloc[1]["pnl"] == 200.0
    
    def test_equity_curve_dataframe(self):
        """Test de génération de la courbe d'équité"""
        # Ajouter des trades pour créer une courbe
        trades = [
            ("trade_1", 50000, 55000, 0.1),  # +500
            ("trade_2", 55000, 52000, 0.1),  # -300
            ("trade_3", 52000, 54000, 0.1),  # +200
        ]
        
        for i, (trade_id, entry, exit, qty) in enumerate(trades):
            self.tracker.add_trade_entry(trade_id, "BTCUSD", "buy", entry, qty)
            self.tracker.close_trade(trade_id, exit)
        
        df = self.tracker.get_equity_curve_df()
        
        assert len(df) == 3
        assert "equity" in df.columns
        assert "pnl" in df.columns
        assert "pnl_pct" in df.columns
        assert df.iloc[0]["equity"] == 10500.0
        assert df.iloc[1]["equity"] == 10200.0
        assert df.iloc[2]["equity"] == 10400.0
    
    def test_performance_summary_report(self):
        """Test du rapport de synthèse"""
        # Ajouter quelques trades
        self.tracker.add_trade_entry("trade_1", "BTCUSD", "buy", 50000, 0.1)
        self.tracker.close_trade("trade_1", 55000)
        
        report = self.tracker.export_summary_report()
        
        assert "strategy_id" in report
        assert "summary" in report
        assert "performance_metrics" in report
        assert "trade_stats" in report
        assert report["summary"]["total_pnl"] == 500.0
        assert report["summary"]["win_rate"] == 100.0
    
    def test_risk_metrics_calculation(self):
        """Test de calcul des métriques de risque"""
        # Créer une série de trades avec variance
        np.random.seed(42)  # Pour reproductibilité
        
        for i in range(10):
            trade_id = f"trade_{i}"
            entry_price = 50000 + np.random.normal(0, 1000)
            exit_price = entry_price + np.random.normal(100, 500)  # Légèrement profitable en moyenne
            
            self.tracker.add_trade_entry(trade_id, "BTCUSD", "buy", entry_price, 0.1)
            self.tracker.close_trade(trade_id, exit_price)
        
        metrics = self.tracker.calculate_metrics()
        
        # Vérifier que les métriques de risque sont calculées
        assert metrics.sharpe_ratio != 0
        assert metrics.profit_factor > 0
        assert metrics.expectancy != 0
    
    def test_consistency_metrics(self):
        """Test des métriques de consistance"""
        # Créer une séquence spécifique: 3 gains, 2 pertes, 4 gains
        results = [500, 300, 200, -150, -100, 400, 250, 300, 150]
        
        for i, pnl in enumerate(results):
            trade_id = f"trade_{i}"
            entry_price = 50000
            exit_price = entry_price + (pnl / 0.1)  # qty = 0.1
            
            self.tracker.add_trade_entry(trade_id, "BTCUSD", "buy", entry_price, 0.1)
            self.tracker.close_trade(trade_id, exit_price)
        
        metrics = self.tracker.calculate_metrics()
        
        assert metrics.max_consecutive_wins == 4  # Les 4 derniers
        assert metrics.max_consecutive_losses == 2  # Les 2 du milieu
    
    def test_cache_invalidation(self):
        """Test d'invalidation du cache des métriques"""
        # Calculer métriques initial
        metrics1 = self.tracker.calculate_metrics()
        
        # Ajouter un trade
        self.tracker.add_trade_entry("trade_1", "BTCUSD", "buy", 50000, 0.1)
        self.tracker.close_trade("trade_1", 55000)
        
        # Les métriques doivent être recalculées
        metrics2 = self.tracker.calculate_metrics()
        
        assert metrics1.total_trades == 0
        assert metrics2.total_trades == 1
        assert metrics2.total_pnl == 500.0


if __name__ == "__main__":
    pytest.main([__file__])