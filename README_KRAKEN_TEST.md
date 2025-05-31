# 🧪 Test Performance Tracking avec Données Kraken

## 📋 Vue d'ensemble

Ce test démontre le système de performance tracking avec **2 sessions** et **2 stratégies par session**, utilisant des données fictives réalistes du `kraken_connector`.

## 🎯 Structure du Test

### 📊 Sessions de Test

#### 1. **Kraken Live Trading Session** (Mode: `live`)
- **BTC Scalping Live** (XXBTZUSD, 1m)
- **ETH Swing Live** (XETHZUSD, 1h)

#### 2. **Kraken Paper Trading Session** (Mode: `paper`)
- **ADA Breakout Paper** (ADAUSD, 5m)
- **DOT Momentum Paper** (DOTUSD, 15m)

## 📈 Données Kraken Simulées

### Prix et Volume Réalistes
| Paire | Prix | Volume 24h | Spread | Timeframe |
|-------|------|------------|---------|-----------|
| XXBTZUSD | $50,123.45 | 1,425.12 | 0.004% | 1m |
| XETHZUSD | $3,012.34 | 2,567.89 | 0.009% | 1h |
| ADAUSD | $0.46 | 234,567.89 | 0.088% | 5m |
| DOTUSD | $6.79 | 567,890.12 | 0.059% | 15m |

### Structure des Données Kraken
```yaml
kraken_data:
  last_price: 50123.45
  pair: XXBTZUSD
  volume_24h: 1425.123456
  high_24h: 50567.89
  low_24h: 49234.56
  bid: 50123.30
  ask: 50123.50
  spread_pct: 0.004
  trades_24h: 8934
```

## 🏆 Résultats de Performance

### 🥇 Sessions les Plus Profitables
1. **Kraken Live Trading Session**: $2,724.19 (2 stratégies)
2. **Kraken Paper Trading Session**: $1,691.34 (2 stratégies)

### 🎯 Meilleures Stratégies Globales
1. **BTC Scalping Live**: $2,847.65 (86.6% win rate, 134 trades)
2. **DOT Momentum Paper**: $1,456.78 (82.2% win rate, 45 trades)
3. **ADA Breakout Paper**: $234.56 (83.3% win rate, 18 trades)
4. **ETH Swing Live**: -$123.46 (73.9% win rate, 23 trades)

## 🚀 Exécution des Tests

### 1. Test Complet Automatisé
```bash
python test_kraken_performance.py
```

### 2. CLI Interactif de Test
```bash
# Informations générales
python test_cli_kraken.py info

# Sessions les plus profitables
python test_cli_kraken.py performance profitable-sessions

# Meilleures stratégies (global)
python test_cli_kraken.py performance best-strategies

# Meilleures stratégies par session
python test_cli_kraken.py performance best-strategies --session-id kraken_live_session

# Tri par métrique spécifique
python test_cli_kraken.py performance best-strategies --metric profit_factor
python test_cli_kraken.py performance best-strategies --metric win_rate

# Détails d'une stratégie
python test_cli_kraken.py strategy show btc_scalping_live
```

## 📊 Métriques Disponibles

### Performance par Stratégie
- **P&L Total** et **Pourcentage**
- **Win Rate** (Taux de réussite)
- **Profit Factor** (Ratio gains/pertes)
- **Max Drawdown** (Perte maximale)
- **Sharpe Ratio** (Rendement ajusté au risque)
- **Nombre de Trades** (Gagnants/Perdants)

### Performance par Session
- **P&L Total** de toutes les stratégies
- **P&L Moyen** par stratégie
- **Meilleure Stratégie** individuelle
- **Nombre de Stratégies** actives

## 🛡️ Risk Management

Chaque stratégie intègre une gestion de risque complète :

```yaml
risk_management:
  max_position_size: 0.05      # Taille max de position
  stop_loss_percent: 0.8       # Stop loss en %
  take_profit_percent: 2.4     # Take profit en %
  risk_ratio: 3.0              # Ratio risque/récompense
  max_daily_loss: 500.0        # Perte journalière max
  max_concurrent_trades: 5     # Trades simultanés max
```

## 🎨 Fonctionnalités Visuelles

### Icônes de Performance
- 🥇🥈🥉 Rankings avec médailles
- 📈📉 Indicateurs P&L (positif/négatif)
- 🎯📊💪 Métriques avec émojis
- 🚀⚪ Status workflow (activé/désactivé)

### Affichage Coloré et Structuré
- Tableaux formatés avec `tabulate`
- Sections clairement délimitées
- Informations hiérarchisées
- Champions mis en évidence

## 🔧 Configuration

### Fichiers de Configuration
- **Production**: `config/trading_config.yaml`
- **Test**: `config/test_trading_config.yaml`

### Variables Kraken Intégrées
Chaque stratégie contient des données Kraken réalistes simulées pour un test complet du système d'intégration.

## ✅ Validation du Système

Ce test valide :
- ✅ **Tracking de performance** en temps réel
- ✅ **Rankings** par différentes métriques
- ✅ **Filtrage** par session ou global
- ✅ **Intégration Kraken** avec données réalistes
- ✅ **Risk Management** par stratégie
- ✅ **Sauvegarde** des meilleures performances
- ✅ **CLI** intuitif et complet

## 🎯 Cas d'Usage Testés

1. **Scalping haute fréquence** (BTC, 1m)
2. **Swing trading** moyen terme (ETH, 1h)
3. **Breakout** sur altcoins (ADA, 5m)
4. **Momentum** avec MACD (DOT, 15m)

Chaque stratégie utilise des paramètres et timeframes différents pour valider la polyvalence du système.