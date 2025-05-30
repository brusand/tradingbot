# Guide des Fonctionnalités de Performance du CLI Trading

## Vue d'ensemble

Le CLI Trading a été enrichi avec des fonctionnalités avancées de suivi et d'analyse des performances, intégrant notre système `PerformanceTracker` pour offrir des métriques complètes et des analyses professionnelles.

## 🎯 Commandes Disponibles

### CLI v1 - Commandes de Base Améliorées

#### `show_session` - Affichage Détaillé des Sessions

```bash
# Affichage de base d'une session
python -m interfaces.cli show_session <session_id>

# Affichage avec métriques détaillées
python -m interfaces.cli show_session <session_id> --detailed

# Affichage avec historique des trades
python -m interfaces.cli show_session <session_id> --trades

# Export du rapport en JSON
python -m interfaces.cli show_session <session_id> --detailed --trades --export report.json

# Toutes les options combinées
python -m interfaces.cli show_session <session_id> --detailed --trades --export complete_report.json
```

**Exemple de sortie :**
```
================================================================================
📊 SESSION DETAILS: My Trading Session
================================================================================
🆔 ID: 12345-abcde-67890
📝 Name: My Trading Session
🎯 Mode: paper
🟢 Status: running
📅 Created: 2025-05-30 10:30:00
🔄 Updated: 2025-05-30 15:45:00

🎯 STRATEGY CONFIGURATION:
  📈 Name: AdvancedTradingStrategy
  💰 Pairs: BTCUSD
  ⏰ Timeframe: 5m
  ⚙️ Parameters: {'short_window': 10, 'long_window': 30}

🛡️ RISK MANAGEMENT:
  📏 Max Position Size: 0.1
  🛑 Stop Loss: 2.0%
  🎯 Take Profit: 4.0%
  💸 Max Daily Loss: 10.0
  📊 Max Exposure: 50.0%

💰 BASIC PERFORMANCE:
  🟢 Total PnL: 150.75
  📊 Total Trades: 24
  🎯 Win Rate: 62.5%

📊 ADVANCED PERFORMANCE METRICS:
  💵 Current Balance: 10150.75
  📈 ROI: 1.51%
  📉 Max Drawdown: 3.2%
  🎯 Sharpe Ratio: 1.245
  📊 Profit Factor: 1.89
  🏆 Winning Trades: 15
  💔 Losing Trades: 9
  📊 Avg Win: 25.30
  📉 Avg Loss: -18.45
  🔥 Max Consecutive Wins: 5
  ❄️ Max Consecutive Losses: 2
```

#### `performance` - Résumé des Performances

```bash
# Top 10 sessions par PnL
python -m interfaces.cli performance

# Top 5 sessions par taux de réussite
python -m interfaces.cli performance --limit 5 --sort-by win_rate

# Top sessions par nombre de trades
python -m interfaces.cli performance --sort-by trades

# Sessions les plus récentes
python -m interfaces.cli performance --sort-by created
```

#### `compare` - Comparaison de Sessions

```bash
# Comparer 2 sessions
python -m interfaces.cli compare session_id_1 session_id_2

# Comparer plusieurs sessions
python -m interfaces.cli compare session_id_1 session_id_2 session_id_3 session_id_4
```

### CLI v2 - Analytics Avancés

#### `analytics portfolio_report` - Rapport de Portefeuille

```bash
# Rapport de portefeuille standard
python -m interfaces.cli_v2 analytics portfolio_report

# Rapport avec filtre sur les trades minimum
python -m interfaces.cli_v2 analytics portfolio_report --min-trades 10

# Rapport avec export JSON
python -m interfaces.cli_v2 analytics portfolio_report --export portfolio_report.json

# Analyse sur 60 jours avec export
python -m interfaces.cli_v2 analytics portfolio_report --days 60 --min-trades 5 --export detailed_report.json
```

**Exemple de sortie :**
```
📊 PORTFOLIO PERFORMANCE REPORT
==================================================
📈 PORTFOLIO SUMMARY:
  🎯 Active Sessions: 8
  💰 Total PnL: 1,245.30
  📊 Total Trades: 156
  📊 Avg PnL/Trade: 7.98

📋 SESSION BREAKDOWN:
Name                     Mode     PnL         Trades   Win Rate   Strategy
--------------------------------------------------------------------------------
BTC_Scalping            paper    🟢 456.20   45       🎯 68.9%   AdvancedTradingStrategy
ETH_Swing               paper    🟢 234.50   28       📈 57.1%   SimpleMovingAverage
Multi_Pair_Bot          live     🟢 123.60   38       🎯 63.2%   AdvancedTradingStrategy
Risk_Averse             paper    🔴 -89.30   15       📉 33.3%   ConservativeStrategy

⚠️ RISK ANALYSIS:
  📈 Profitable Sessions: 6/8 (75.0%)
  📉 Largest Single Loss: -89.30
```

#### `analytics compare_advanced` - Comparaison Avancée

```bash
# Comparaison avancée par PnL
python -m interfaces.cli_v2 analytics compare_advanced session1 session2 session3

# Comparaison par taux de réussite
python -m interfaces.cli_v2 analytics compare_advanced session1 session2 --metric win_rate

# Comparaison par nombre de trades
python -m interfaces.cli_v2 analytics compare_advanced session1 session2 --metric trades
```

#### `analytics trend_analysis` - Analyse des Tendances

```bash
# Analyse des tendances quotidiennes
python -m interfaces.cli_v2 analytics trend_analysis

# Analyse hebdomadaire
python -m interfaces.cli_v2 analytics trend_analysis --timeframe weekly

# Analyse filtrée par stratégies
python -m interfaces.cli_v2 analytics trend_analysis --strategies AdvancedTradingStrategy SimpleMovingAverage
```

#### `analytics correlation_matrix` - Analyse de Corrélation

```bash
# Analyse de corrélation standard (seuil 0.7)
python -m interfaces.cli_v2 analytics correlation_matrix

# Analyse avec seuil personnalisé
python -m interfaces.cli_v2 analytics correlation_matrix --min-correlation 0.6
```

## 📊 Métriques Disponibles

### Métriques de Base
- **Total PnL** : Gains/pertes totaux
- **Nombre de Trades** : Total des transactions
- **Taux de Réussite** : Pourcentage de trades gagnants
- **Balance Actuelle** : Capital actuel

### Métriques de Risque
- **Max Drawdown** : Perte maximale depuis un pic
- **Drawdown Actuel** : Perte actuelle depuis le dernier pic
- **Sharpe Ratio** : Rendement ajusté au risque
- **Profit Factor** : Ratio gains/pertes bruts
- **VaR 95%** : Value at Risk à 95%

### Métriques de Consistance
- **Séquences Gagnantes/Perdantes** : Maximum et actuelles
- **Trades Moyens** : Gains/pertes moyens
- **Plus Gros Gain/Perte** : Extremes de performance
- **Expectancy** : Espérance mathématique par trade

## 🎨 Fonctionnalités Visuelles

### Émojis et Indicateurs Visuels
- 🟢 **Vert** : Performances positives
- 🔴 **Rouge** : Performances négatives
- 🎯 **Cible** : Excellent taux de réussite (≥60%)
- 📈 **Graphique montant** : Bon taux de réussite (40-60%)
- 📉 **Graphique descendant** : Faible taux de réussite (<40%)
- 🏆 **Trophée** : Meilleure performance
- ⚠️ **Avertissement** : Attention/risque

### Status des Sessions
- 🟢 **Running/Active** : Session en cours
- 🟡 **Paused** : Session en pause
- 🔴 **Stopped** : Session arrêtée
- ⚪ **Created** : Session créée mais non démarrée
- ❌ **Error** : Erreur

## 💾 Export et Intégration

### Formats d'Export
```json
{
  "session_id": "12345-abcde",
  "session_name": "Demo Trading Session",
  "mode": "paper",
  "status": "running",
  "strategy": {
    "name": "AdvancedTradingStrategy",
    "pairs": ["BTCUSD"],
    "timeframe": "5m"
  },
  "performance": {
    "total_pnl": 150.75,
    "total_trades": 24,
    "win_rate": 0.625,
    "sharpe_ratio": 1.245,
    "max_drawdown_pct": 3.2
  },
  "exported_at": "2025-05-30T15:45:00Z"
}
```

### Intégration avec des Outils Externes
```bash
# Export vers Excel/Google Sheets
python -m interfaces.cli show_session <id> --export data.json
# Puis utiliser un convertisseur JSON->CSV

# Intégration avec des dashboards
curl -X POST https://your-dashboard.com/api/upload \
  -F "file=@report.json" \
  -H "Authorization: Bearer YOUR_TOKEN"

# Analyse avec Python/Pandas
import json
import pandas as pd

with open('report.json', 'r') as f:
    data = json.load(f)
    
df = pd.DataFrame(data['sessions'])
print(df.describe())
```

## 🔧 Cas d'Usage Pratiques

### 1. Suivi Quotidien
```bash
#!/bin/bash
# Script de monitoring quotidien

echo "📊 Daily Trading Report - $(date)"
echo "=================================="

# Status général
python -m interfaces.cli status

# Top performances
python -m interfaces.cli performance --limit 5

# Sessions actives détaillées
for session in $(python -m interfaces.cli list_sessions | grep running | cut -d' ' -f1); do
    python -m interfaces.cli show_session $session --detailed
done
```

### 2. Analyse Hebdomadaire
```bash
#!/bin/bash
# Rapport hebdomadaire automatisé

DATE=$(date +%Y%m%d)
REPORT_DIR="reports/weekly"
mkdir -p $REPORT_DIR

# Rapport de portefeuille
python -m interfaces.cli_v2 analytics portfolio_report \
    --days 7 \
    --export "$REPORT_DIR/portfolio_$DATE.json"

# Analyse des tendances
python -m interfaces.cli_v2 analytics trend_analysis \
    --timeframe weekly > "$REPORT_DIR/trends_$DATE.txt"

# Corrélations
python -m interfaces.cli_v2 analytics correlation_matrix \
    > "$REPORT_DIR/correlations_$DATE.txt"

echo "📧 Sending weekly report..."
# Intégration avec système de notification
```

### 3. Debugging de Performance
```bash
# Identifier les sessions problématiques
python -m interfaces.cli performance --sort-by pnl | head -n 5

# Analyser en détail
python -m interfaces.cli show_session <problematic_session> --detailed --trades

# Comparer avec une session performante
python -m interfaces.cli compare <problematic_session> <good_session>

# Exporter pour analyse approfondie
python -m interfaces.cli show_session <problematic_session> \
    --detailed --trades --export debug_analysis.json
```

### 4. Optimisation de Stratégies
```bash
# Comparer différentes variantes d'une stratégie
python -m interfaces.cli_v2 analytics compare_advanced \
    strategy_v1 strategy_v2 strategy_v3 \
    --metric win_rate

# Analyser les tendances par stratégie
python -m interfaces.cli_v2 analytics trend_analysis \
    --strategies AdvancedTradingStrategy

# Analyser les corrélations pour éviter la redondance
python -m interfaces.cli_v2 analytics correlation_matrix \
    --min-correlation 0.8
```

## 🎮 Mode Interactif et Watch

### Watch Mode pour Monitoring Temps Réel
```bash
# Surveillance continue (CLI v2)
python -m interfaces.cli_v2 list_running --watch

# Script de monitoring personnalisé
while true; do
    clear
    echo "🎮 LIVE TRADING DASHBOARD - $(date)"
    echo "=================================="
    python -m interfaces.cli status
    echo ""
    python -m interfaces.cli performance --limit 3
    sleep 30
done
```

## 🚀 Prochaines Évolutions

### Fonctionnalités Prévues
1. **Graphiques ASCII** : Courbes d'équité en mode texte
2. **Alertes Intelligentes** : Notifications sur seuils de performance
3. **Backtesting Intégré** : Tests historiques depuis le CLI
4. **Machine Learning** : Suggestions d'optimisation automatiques
5. **API REST** : Exposition des métriques via API

### Intégrations Futures
- **Slack/Discord** : Notifications automatiques
- **Telegram Bot** : Monitoring mobile
- **Grafana** : Dashboards visuels
- **Jupyter Notebooks** : Analyses interactives

Le système CLI offre maintenant une plateforme complète pour le monitoring, l'analyse et l'optimisation des stratégies de trading avec des métriques professionnelles et une expérience utilisateur enrichie.