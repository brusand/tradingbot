# TradingCLI V3 - Guide de Démarrage Rapide

## Installation

Le système est maintenant configuré avec des exemples de stratégies.

## Commandes de Base

### 1. Afficher la version et les fonctionnalités
```bash
python trading_cli.py version
```

### 2. Lister les stratégies configurées
```bash
python trading_cli.py strategy list
```

### 3. Afficher les détails d'une stratégie
```bash
python trading_cli.py strategy show demo_sma_crossover
```

### 4. Démarrer le système de workflow avancé
```bash
python trading_cli.py system start
```

### 5. Vérifier le statut du système
```bash
python trading_cli.py system status
```

### 6. Démarrer une stratégie dans le workflow
```bash
python trading_cli.py strategy start demo_sma_crossover
```

### 7. Voir le statut en temps réel d'une stratégie
```bash
python trading_cli.py strategy status demo_sma_crossover
```

### 8. Tester le workflow avec des données simulées
```bash
python trading_cli.py test workflow --duration 60
```

### 9. Créer une nouvelle stratégie
```bash
python trading_cli.py strategy create --workflow
```

### 10. Gérer les sessions
```bash
python trading_cli.py session list
python trading_cli.py session show demo_session
python trading_cli.py session start demo_session
```

## Workflow Avancé

Le système intègre un workflow de traitement avancé avec :

- **Queue de traitement séquentiel** : Traitement ordonné des candles
- **Calculs parallèles d'indicateurs** : RSI, SMA, EMA, MACD en parallèle
- **Interpréteur de conditions** : Syntaxe avancée pour les signaux
- **DataFrame temps réel** : Toutes les données dans un DataFrame unifié
- **Métriques de performance** : Monitoring en temps réel

### Exemples de Conditions de Signaux

```
# Crossover SMA avec RSI
SMA_10 > SMA_30 & SMA_10[-1] <= SMA_30[-1] & RSI_14 < 70

# Tendance EMA avec momentum
EMA_12 > EMA_26 & close > EMA_12 & RSI_14 > 50 & RSI_14 < 80

# Signal avec référence temporelle
close > SMA_20 & SMA_20 > SMA_20[-1] & RSI_14 > RSI_14[-1]
```

## Flux de Travail Typique

1. **Démarrer le système** : `system start`
2. **Configurer une stratégie** : `strategy create` et `strategy configure`
3. **Ajouter des indicateurs** : `indicator add`
4. **Démarrer la stratégie** : `strategy start`
5. **Monitorer** : `strategy status` et `system metrics`
6. **Tester** : `test workflow`

## Fonctionnalités Avancées

- **Mode papier trading** : Test sans risque
- **Gestion des risques** : Profils de risk management
- **Sessions multiples** : Gestion de plusieurs configurations
- **Métriques temps réel** : Performance tracking
- **Communication asynchrone** : PubSub entre composants

Pour plus d'aide, utilisez `--help` avec n'importe quelle commande.
