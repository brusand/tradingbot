# Système de Suivi de Performance

## Vue d'ensemble

Le système de suivi de performance offre un tracking complet et en temps réel des résultats des stratégies de trading. Il calcule automatiquement plus de 30 métriques de performance clés et fournit des analyses détaillées des trades.

## Fonctionnalités Principales

### 📊 Métriques de Performance Complètes

#### Métriques de Base
- **PnL Total** : Gains/pertes totaux en valeur absolue et pourcentage
- **Nombre de Trades** : Total, gagnants, perdants
- **Taux de Réussite** : Pourcentage de trades gagnants
- **Trade Moyen** : PnL moyen par trade

#### Métriques de Risque
- **Max Drawdown** : Perte maximale depuis un pic (absolu et %)
- **Drawdown Actuel** : Perte actuelle depuis le dernier pic
- **Sharpe Ratio** : Rendement ajusté au risque
- **Sortino Ratio** : Sharpe focalisé sur la volatilité négative
- **VaR 95%** : Value at Risk à 95%
- **Profit Factor** : Ratio gains/pertes bruts

#### Métriques de Consistance
- **Séquences Gagnantes/Perdantes** : Maximum et actuelles
- **Expectancy** : Espérance mathématique par trade
- **Recovery Factor** : Capacité de récupération après drawdown

#### Métriques Temporelles
- **Temps de Détention Moyen** : Par trade, gains, pertes
- **Durée Max Drawdown** : Temps pour récupérer

### 🎯 Suivi des Trades en Temps Réel

#### Enregistrement Automatique
```python
# Ouverture de trade automatique lors de signaux
trade = performance_tracker.add_trade_entry(
    trade_id="unique_id",
    symbol="BTCUSD",
    side="buy",  # ou "sell"
    price=50000.0,
    quantity=0.1,
    signal_id="signal_xyz"
)

# Fermeture automatique
closed_trade = performance_tracker.close_trade("unique_id", exit_price=55000.0)
```

#### Suivi des Excursions
- **MFE (Max Favorable Excursion)** : Plus gros gain potentiel atteint
- **MAE (Max Adverse Excursion)** : Plus grosse perte temporaire subie

### 📈 Analyses et Rapports

#### Historique des Trades
```python
# DataFrame complet des trades
trade_history = strategy.get_trade_history()
# Colonnes: trade_id, symbol, side, entry_price, exit_price, pnl, pnl_pct, etc.
```

#### Courbe d'Équité
```python
# Évolution du capital dans le temps
equity_curve = strategy.get_equity_curve()
# Colonnes: timestamp, equity, pnl, pnl_pct
```

#### Rendements Mensuels
```python
# Analyse des performances par mois
monthly_returns = strategy.get_monthly_returns()
```

## Intégration avec les Stratégies

### AdvancedTradingStrategy

La stratégie avancée intègre automatiquement le système de performance :

```python
# Initialisation automatique
strategy = AdvancedTradingStrategy(config, connector)
await strategy.start()

# Le tracker est automatiquement créé
assert strategy.performance_tracker.strategy_id == config.name

# Accès aux métriques
summary = strategy.get_performance_summary()
metrics = strategy.get_performance_metrics()
```

### Conditions de Sortie Automatiques

Le système inclut des exemples de conditions de sortie :

```python
def simulate_exit_conditions(self):
    """Conditions de sortie automatiques"""
    current_price = self.dataframe.iloc[-1]['close']
    current_rsi = self.dataframe.iloc[-1].get('RSI_14', 50)
    
    for trade_id, trade in self.performance_tracker.open_trades.items():
        # Stop Loss à -5%
        if self.calculate_loss_pct(trade, current_price) < -5:
            self.close_trade_by_id(trade_id, current_price, "stop_loss")
        
        # Take Profit sur RSI
        elif (trade.side == "buy" and current_rsi > 70) or \
             (trade.side == "sell" and current_rsi < 30):
            self.close_trade_by_id(trade_id, current_price, "take_profit_rsi")
```

## Métriques Clés Expliquées

### Sharpe Ratio
Mesure le rendement par unité de risque. Plus le ratio est élevé, meilleure est la performance ajustée au risque.
```
Sharpe = Rendement Moyen / Écart-Type des Rendements
```

### Profit Factor
Ratio entre les gains bruts et les pertes brutes. Un ratio > 1 indique une stratégie profitable.
```
Profit Factor = Somme des Gains / Somme des Pertes (absolue)
```

### Max Drawdown
Perte maximale depuis un pic de performance. Mesure cruciale du risque de la stratégie.
```
Max DD = (Peak Value - Trough Value) / Peak Value × 100%
```

### Expectancy
Gain/perte moyen attendu par trade.
```
Expectancy = (Win Rate × Avg Win) - (Loss Rate × Avg Loss)
```

## Exemple d'Utilisation

### Rapport de Performance Complet

```python
# Obtenir le rapport complet
report = strategy.get_performance_summary()

print(f"Stratégie: {report['strategy_id']}")
print(f"PnL Total: ${report['summary']['total_pnl']:+,.2f}")
print(f"Taux de Réussite: {report['summary']['win_rate']:.1f}%")
print(f"Max Drawdown: {report['risk_metrics']['max_drawdown_pct']:.2f}%")
print(f"Sharpe Ratio: {report['performance_metrics']['sharpe_ratio']:.2f}")
print(f"Profit Factor: {report['performance_metrics']['profit_factor']:.2f}")
```

### Analyse Temporelle

```python
# Analyse des trades par période
equity_df = strategy.get_equity_curve()
monthly_returns = strategy.get_monthly_returns()

# Visualisation simple de la courbe d'équité
for i, row in equity_df.iterrows():
    print(f"Trade {i+1}: ${row['equity']:,.0f} ({row['pnl_pct']:+.1f}%)")
```

## Optimisation des Performances

### Cache Intelligent
- Les métriques sont mises en cache pendant 60 secondes
- Recalcul automatique lors de nouveaux trades
- Invalidation intelligente du cache

### Gestion Mémoire
- Limitation automatique à 1000 dernières chandelles
- Weak references dans PubSub pour éviter les fuites mémoire
- Nettoyage automatique des données obsolètes

## Tests et Validation

Le système inclut une suite de tests complète :

```bash
# Tests du tracker de performance
pytest tests/test_performance_tracker.py -v

# Tests d'intégration avec les stratégies  
pytest tests/test_advanced_strategy.py::TestAdvancedTradingStrategy::test_performance_tracking_integration -v

# Démonstration complète
python examples/performance_demo.py
```

## Rapport d'Exemple

```
📊 RAPPORT DE PERFORMANCE DÉTAILLÉ
Stratégie: ma_strategie
Période: 2025-01-01 → 2025-05-30

💰 RÉSULTATS FINANCIERS
Capital initial: $50,000.00
Capital final: $65,432.10
PnL total: $+15,432.10
Rendement: +30.86%

📈 STATISTIQUES DE TRADING
Nombre total de trades: 156
Taux de réussite: 68.6%
Trades gagnants: 107
Trades perdants: 49
Gain moyen: $+234.50
Perte moyenne: $-128.30

⚠️  MÉTRIQUES DE RISQUE
Drawdown maximum: $3,240.00 (5.12%)
Sharpe Ratio: 2.34
Profit Factor: 2.89

🔄 CONSISTANCE
Gains consécutifs max: 12
Pertes consécutives max: 3
```

## Prochaines Évolutions Possibles

1. **Backtesting Intégré** : Extension pour backtests historiques
2. **Optimisation de Paramètres** : Suggestions automatiques d'amélioration
3. **Alertes Intelligentes** : Notifications sur seuils de performance
4. **Export Avancé** : Rapports PDF, graphiques, Excel
5. **Comparaison de Stratégies** : Benchmarking entre stratégies
6. **Machine Learning** : Prédiction de performance basée sur l'historique

## Architecture Technique

```
PerformanceTracker
├── TradeRecord (dataclass)
├── PerformanceMetrics (dataclass)  
├── Méthodes de calcul
│   ├── _calculate_drawdown_metrics()
│   ├── _calculate_risk_metrics()
│   └── _calculate_consistency_metrics()
├── Cache intelligent
└── Export/Import données

AdvancedTradingStrategy
├── PerformanceTracker intégré
├── Enregistrement auto des trades
├── Conditions de sortie
└── API d'accès aux métriques
```

Ce système de performance offre une base solide pour l'évaluation et l'optimisation des stratégies de trading, avec des métriques professionnelles et une intégration transparente.