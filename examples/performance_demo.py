#!/usr/bin/env python3
"""
Démonstration du système de suivi de performance
"""

import asyncio
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, UTC, timedelta

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.performance_tracker import PerformanceTracker
from strategies.advanced_strategy import AdvancedTradingStrategy
from data.models import StrategyConfig


async def demo_performance_tracker():
    """Démonstration du tracker de performance"""
    print("=== Démonstration du Performance Tracker ===\n")
    
    # Initialiser le tracker
    tracker = PerformanceTracker("demo_strategy", initial_capital=10000.0)
    
    print(f"Capital initial: ${tracker.initial_capital:,.2f}")
    print(f"ID Stratégie: {tracker.strategy_id}\n")
    
    # Simuler une série de trades
    trades_data = [
        # (trade_id, symbol, side, entry_price, exit_price, quantity)
        ("trade_1", "BTCUSD", "buy", 50000, 55000, 0.1),    # +$500 
        ("trade_2", "ETHUSD", "buy", 3000, 2800, 1.0),      # -$200
        ("trade_3", "BTCUSD", "sell", 52000, 48000, 0.1),   # +$400
        ("trade_4", "ADAUSD", "buy", 1.0, 1.2, 1000),       # +$200
        ("trade_5", "BTCUSD", "buy", 48000, 46000, 0.1),    # -$200
        ("trade_6", "ETHUSD", "buy", 2800, 3200, 1.0),      # +$400
        ("trade_7", "BTCUSD", "sell", 47000, 45000, 0.1),   # +$200
        ("trade_8", "DOGUSD", "buy", 0.5, 0.45, 2000),      # -$100
    ]
    
    print("Simulation de trades...")
    for trade_id, symbol, side, entry, exit, qty in trades_data:
        # Ajouter le trade
        trade = tracker.add_trade_entry(
            trade_id=trade_id,
            symbol=symbol,
            side=side,
            price=entry,
            quantity=qty,
            signal_id=f"signal_{trade_id}"
        )
        
        # Simuler quelques excursions avant la fermeture
        if side == "buy":
            max_price = exit + (exit - entry) * 0.3  # 30% de plus que le profit
            min_price = entry - abs(exit - entry) * 0.5  # 50% de perte potentielle
        else:
            min_price = exit - (entry - exit) * 0.3
            max_price = entry + abs(entry - exit) * 0.5
        
        tracker.update_open_trade_excursion(trade_id, max_price)
        tracker.update_open_trade_excursion(trade_id, min_price)
        
        # Fermer le trade
        closed_trade = tracker.close_trade(trade_id, exit)
        
        pnl_sign = "✅" if closed_trade.pnl > 0 else "❌"
        print(f"  {pnl_sign} {trade_id}: {symbol} {side.upper()} @ ${entry:,.0f} → ${exit:,.0f} = ${closed_trade.pnl:+.2f}")
    
    print(f"\nCapital final: ${tracker.current_capital:,.2f}")
    print(f"PnL total: ${tracker.current_capital - tracker.initial_capital:+,.2f}\n")
    
    # Calculer et afficher les métriques
    metrics = tracker.calculate_metrics()
    
    print("=== MÉTRIQUES DE PERFORMANCE ===")
    print(f"Nombre total de trades: {metrics.total_trades}")
    print(f"Trades gagnants: {metrics.winning_trades} ({metrics.win_rate:.1f}%)")
    print(f"Trades perdants: {metrics.losing_trades}")
    print(f"PnL total: ${metrics.total_pnl:+,.2f} ({metrics.total_pnl_pct:+.2f}%)")
    print(f"Trade moyen: ${metrics.avg_trade:+,.2f}")
    print(f"Plus gros gain: ${metrics.largest_win:+,.2f}")
    print(f"Plus grosse perte: ${metrics.largest_loss:+,.2f}")
    print()
    
    print("=== MÉTRIQUES DE RISQUE ===")
    print(f"Drawdown maximum: ${metrics.max_drawdown:,.2f} ({metrics.max_drawdown_pct:.2f}%)")
    print(f"Drawdown actuel: ${metrics.current_drawdown:,.2f} ({metrics.current_drawdown_pct:.2f}%)")
    print(f"Profit Factor: {metrics.profit_factor:.2f}")
    print(f"Sharpe Ratio: {metrics.sharpe_ratio:.2f}")
    print(f"Expectancy: ${metrics.expectancy:+.2f}")
    print(f"Recovery Factor: {metrics.recovery_factor:.2f}")
    print()
    
    print("=== MÉTRIQUES DE CONSISTANCE ===")
    print(f"Gains consécutifs max: {metrics.max_consecutive_wins}")
    print(f"Pertes consécutives max: {metrics.max_consecutive_losses}")
    print(f"Gains consécutifs actuels: {metrics.consecutive_wins}")
    print(f"Pertes consécutives actuelles: {metrics.consecutive_losses}")
    print()
    
    # Afficher l'historique des trades
    print("=== HISTORIQUE DES TRADES ===")
    trade_history = tracker.get_trade_history_df()
    if not trade_history.empty:
        # Sélectionner les colonnes importantes pour l'affichage
        display_cols = ['trade_id', 'symbol', 'side', 'entry_price', 'exit_price', 'pnl', 'pnl_pct', 'holding_time_hours']
        print(trade_history[display_cols].to_string(index=False, float_format='%.2f'))
    print()
    
    # Afficher la courbe d'équité
    print("=== COURBE D'ÉQUITÉ ===")
    equity_curve = tracker.get_equity_curve_df()
    if not equity_curve.empty:
        print("Évolution du capital:")
        for i, row in equity_curve.iterrows():
            print(f"  Trade {i+1}: ${row['equity']:,.2f} (PnL: ${row['pnl']:+,.2f})")
    print()
    
    # Exporter le rapport complet
    print("=== RAPPORT DE SYNTHÈSE ===")
    report = tracker.export_summary_report()
    
    print("Résumé:")
    for key, value in report['summary'].items():
        if isinstance(value, float):
            print(f"  {key}: {value:+,.2f}")
        else:
            print(f"  {key}: {value}")
    
    return tracker


async def demo_advanced_strategy_performance():
    """Démonstration avec la stratégie avancée"""
    print("\n" + "="*60)
    print("=== Démonstration avec Stratégie Avancée ===")
    print("="*60 + "\n")
    
    # Configuration de stratégie
    config = StrategyConfig(
        name="demo_advanced_strategy",
        parameters={"demo": True},
        timeframe="5m",
        pairs=["BTCUSD"]
    )
    
    # Mock connector simple
    class MockConnector:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def place_order(self, *args, **kwargs):
            return "mock_order_123"
    
    # Créer la stratégie
    strategy = AdvancedTradingStrategy(config, MockConnector())
    await strategy.start()
    
    print(f"Stratégie démarrée: {strategy.config.name}")
    print(f"Capital initial: ${strategy.performance_tracker.initial_capital:,.2f}")
    print()
    
    # Simuler quelques trades manuels
    trades = [
        ("manual_trade_1", "BTCUSD", "buy", 50000, 52000, 0.05),
        ("manual_trade_2", "BTCUSD", "sell", 51000, 49000, 0.05),
        ("manual_trade_3", "BTCUSD", "buy", 48000, 50000, 0.05),
    ]
    
    print("Simulation de trades avec la stratégie avancée...")
    for trade_id, symbol, side, entry, exit, qty in trades:
        # Ajouter le trade
        strategy.performance_tracker.add_trade_entry(
            trade_id=trade_id,
            symbol=symbol,
            side=side,
            price=entry,
            quantity=qty,
            signal_id=f"manual_{trade_id}"
        )
        
        # Fermer le trade
        await strategy.close_trade_by_id(trade_id, exit, "manual_close")
    
    # Afficher les performances
    print("\nPerformances de la stratégie:")
    summary = strategy.get_performance_summary()
    
    print(f"PnL total: ${summary['summary']['total_pnl']:+,.2f}")
    print(f"Taux de réussite: {summary['summary']['win_rate']:.1f}%")
    print(f"Nombre de trades: {summary['summary']['total_trades']}")
    
    # Métriques détaillées
    metrics = strategy.get_performance_metrics()
    print(f"Sharpe Ratio: {metrics.sharpe_ratio:.2f}")
    print(f"Profit Factor: {metrics.profit_factor:.2f}")
    print(f"Drawdown max: {metrics.max_drawdown_pct:.2f}%")
    
    await strategy.stop()
    print("\nStratégie arrêtée.")


def create_sample_performance_report():
    """Créer un rapport de performance d'exemple"""
    print("\n" + "="*60)
    print("=== Exemple de Rapport de Performance ===")
    print("="*60)
    
    # Simuler des données de performance réalistes
    np.random.seed(42)
    
    tracker = PerformanceTracker("sample_strategy", 50000.0)
    
    # Générer 100 trades aléatoires mais réalistes
    symbols = ["BTCUSD", "ETHUSD", "ADAUSD", "DOTUSD", "LINKUSD"]
    
    for i in range(100):
        symbol = np.random.choice(symbols)
        side = np.random.choice(["buy", "sell"])
        
        # Prix d'entrée réaliste selon le symbole
        if "BTC" in symbol:
            entry_price = np.random.uniform(45000, 55000)
            price_change_pct = np.random.normal(0.02, 0.05)  # 2% moyen, 5% volatilité
        elif "ETH" in symbol:
            entry_price = np.random.uniform(2500, 3500)
            price_change_pct = np.random.normal(0.015, 0.06)
        else:
            entry_price = np.random.uniform(0.5, 2.0)
            price_change_pct = np.random.normal(0.01, 0.08)
        
        exit_price = entry_price * (1 + price_change_pct)
        quantity = np.random.uniform(0.01, 0.1) if "BTC" in symbol else np.random.uniform(0.1, 2.0)
        
        trade_id = f"sample_trade_{i+1:03d}"
        
        tracker.add_trade_entry(trade_id, symbol, side, entry_price, quantity)
        tracker.close_trade(trade_id, exit_price)
    
    # Générer le rapport
    report = tracker.export_summary_report()
    
    print("📊 RAPPORT DE PERFORMANCE DÉTAILLÉ")
    print("-" * 40)
    print(f"Stratégie: {report['strategy_id']}")
    print(f"Période: {report['periods']['first_trade'].strftime('%Y-%m-%d')} → {report['periods']['last_trade'].strftime('%Y-%m-%d')}")
    print()
    
    print("💰 RÉSULTATS FINANCIERS")
    print(f"Capital initial: ${report['summary']['initial_capital']:,.2f}")
    print(f"Capital final: ${report['summary']['current_capital']:,.2f}")
    print(f"PnL total: ${report['summary']['total_pnl']:+,.2f}")
    print(f"Rendement: {report['summary']['total_pnl_pct']:+.2f}%")
    print()
    
    print("📈 STATISTIQUES DE TRADING")
    print(f"Nombre total de trades: {report['summary']['total_trades']}")
    print(f"Taux de réussite: {report['summary']['win_rate']:.1f}%")
    print(f"Trades gagnants: {report['trade_stats']['winning_trades']}")
    print(f"Trades perdants: {report['trade_stats']['losing_trades']}")
    print(f"Gain moyen: ${report['trade_stats']['avg_win']:+,.2f}")
    print(f"Perte moyenne: ${report['trade_stats']['avg_loss']:+,.2f}")
    print(f"Plus gros gain: ${report['trade_stats']['largest_win']:+,.2f}")
    print(f"Plus grosse perte: ${report['trade_stats']['largest_loss']:+,.2f}")
    print()
    
    print("⚠️  MÉTRIQUES DE RISQUE")
    print(f"Drawdown maximum: ${report['risk_metrics']['max_drawdown']:,.2f} ({report['risk_metrics']['max_drawdown_pct']:.2f}%)")
    print(f"Drawdown actuel: {report['risk_metrics']['current_drawdown_pct']:.2f}%")
    print(f"VaR 95%: ${report['risk_metrics']['var_95']:+,.2f}")
    print()
    
    print("📊 RATIOS DE PERFORMANCE")
    print(f"Profit Factor: {report['performance_metrics']['profit_factor']:.2f}")
    print(f"Sharpe Ratio: {report['performance_metrics']['sharpe_ratio']:.2f}")
    print(f"Sortino Ratio: {report['performance_metrics']['sortino_ratio']:.2f}")
    print(f"Expectancy: ${report['performance_metrics']['expectancy']:+.2f}")
    print(f"Recovery Factor: {report['performance_metrics']['recovery_factor']:.2f}")
    print()
    
    print("🔄 CONSISTANCE")
    print(f"Gains consécutifs max: {report['consistency']['max_consecutive_wins']}")
    print(f"Pertes consécutives max: {report['consistency']['max_consecutive_losses']}")
    print()
    
    # Créer un graphique simple en mode texte
    equity_df = tracker.get_equity_curve_df()
    if not equity_df.empty:
        print("📈 COURBE D'ÉQUITÉ (derniers 20 trades)")
        recent_equity = equity_df.tail(20)
        
        min_equity = recent_equity['equity'].min()
        max_equity = recent_equity['equity'].max()
        
        for i, row in recent_equity.iterrows():
            # Normaliser pour l'affichage (0-50 caractères)
            if max_equity > min_equity:
                bar_length = int(((row['equity'] - min_equity) / (max_equity - min_equity)) * 40)
            else:
                bar_length = 20
            
            bar = "█" * bar_length + "░" * (40 - bar_length)
            print(f"Trade {i+1:2d}: ${row['equity']:7,.0f} |{bar}| {row['pnl_pct']:+5.1f}%")
    
    return tracker


async def main():
    """Fonction principale de démonstration"""
    print("🚀 DÉMONSTRATION DU SYSTÈME DE PERFORMANCE TRADING")
    print("=" * 60)
    
    try:
        # Démo 1: Tracker de base
        await demo_performance_tracker()
        
        # Démo 2: Intégration avec stratégie avancée
        await demo_advanced_strategy_performance()
        
        # Démo 3: Rapport détaillé
        create_sample_performance_report()
        
        print("\n✅ Démonstration terminée avec succès!")
        print("\nLe système de suivi de performance offre:")
        print("- 📊 Métriques complètes (PnL, drawdown, ratios)")
        print("- 🎯 Suivi des trades en temps réel") 
        print("- 📈 Courbes d'équité et historiques")
        print("- ⚡ Cache optimisé pour les performances")
        print("- 🔄 Intégration transparente avec les stratégies")
        
    except Exception as e:
        print(f"❌ Erreur lors de la démonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())