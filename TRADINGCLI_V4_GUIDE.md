# TradingCLI V4 - Documentation Complète

## Vue d'ensemble

TradingCLI V4 est la fusion complète de toutes les versions CLI précédentes :
- **CLI de base** : Gestion des sessions et performances
- **CLI V2** : Multi-session et analytics avancés
- **CLI Adapted** : Configuration YAML persistante  
- **CLI V3** : Workflow avancé avec indicateurs parallèles

## Architecture Unifiée

### Composants Fusionnés
- 🏗️ **Système de base** : SessionManager, StrategyEngine
- 🚀 **Workflow avancé** : PubSub, calculs parallèles d'indicateurs
- 📊 **Analytics** : Portfolio, comparaisons, métriques
- 💾 **Configuration** : YAML persistant avec migration automatique
- 🔧 **Multi-session** : Gestion parallèle des sessions

### Fonctionnalités V4

#### 1. Gestion des Sessions Unifiée
```bash
# Création avec options avancées
python trading_cli_v4.py session create --workflow --count 3

# Liste avec filtres
python trading_cli_v4.py session list --mode running

# Affichage temps réel
python trading_cli_v4.py session show session_id --live
```

#### 2. Workflow Avancé Intégré
```bash
# Démarrage système complet
python trading_cli_v4.py system start --mode workflow

# Test workflow avec métriques
python trading_cli_v4.py test workflow --duration 60 --strategies 2

# Monitoring temps réel
python trading_cli_v4.py system status --detailed
```

#### 3. Analytics et Rapports
```bash
# Rapport portfolio complet
python trading_cli_v4.py analytics portfolio --export report.json

# Comparaison multi-sessions
python trading_cli_v4.py analytics compare session1 session2 session3

# Métriques système
python trading_cli_v4.py system metrics --export metrics.json
```

#### 4. Stratégies Avancées
```bash
# Création interactive avec workflow
python trading_cli_v4.py strategy create --workflow --interactive

# Liste avec filtres
python trading_cli_v4.py strategy list --filter-workflow --filter-active

# Configuration enrichie automatique
```

## Migration depuis les versions précédentes

Le système V4 migre automatiquement :
- ✅ Configurations V3 (stratégies, sessions, profils)
- ✅ Sauvegarde automatique des anciennes versions
- ✅ Enrichissement avec nouvelles fonctionnalités V4
- ✅ Compatibilité ascendante complète

## Commandes Principales V4

### Système
```bash
python trading_cli_v4.py version                    # Capacités complètes
python trading_cli_v4.py init --force              # Réinitialisation
python trading_cli_v4.py system start --mode workflow
python trading_cli_v4.py system status --detailed
python trading_cli_v4.py system metrics --export
```

### Sessions
```bash
python trading_cli_v4.py session create --workflow --interactive
python trading_cli_v4.py session list --format json
python trading_cli_v4.py session start session_id --force
python trading_cli_v4.py session show session_id --live
```

### Stratégies
```bash
python trading_cli_v4.py strategy create --workflow --interactive
python trading_cli_v4.py strategy list --filter-workflow
python trading_cli_v4.py strategy start strategy_id
```

### Analytics
```bash
python trading_cli_v4.py analytics portfolio --period 30
python trading_cli_v4.py analytics compare session1 session2
```

### Tests
```bash
python trading_cli_v4.py test workflow --duration 30
python trading_cli_v4.py test multi-session --count 5
```

## Nouveautés V4

### 🔥 Fonctionnalités Exclusives V4
- **Migration automatique** des configurations précédentes
- **Système unifié** combinant tous les managers
- **Configuration hybride** YAML + base de données
- **Analytics temps réel** avec portfolio tracking
- **Multi-mode** : basic, workflow, multi-session
- **Tests intégrés** pour validation complète

### 📊 Interface Enrichie
- Formatage avancé avec émojis et couleurs
- Support multi-format (table, JSON, YAML)
- Mode temps réel avec rafraîchissement
- Filtres et exports avancés

### 🚀 Performance
- Initialisation intelligente des composants
- Cache pour optimiser les performances
- Arrêt propre de tous les composants
- Gestion d'erreurs robuste

## Architecture Technique

```
TradingCLI V4
├── Système de Base
│   ├── SessionManager
│   ├── StrategyEngine
│   └── DatabaseManager
├── Workflow Avancé (optionnel)
│   ├── PubSubEngine
│   ├── StrategyManagerEnhanced
│   └── IndicatorsService
├── Multi-Session (optionnel)
│   └── MultiSessionManager
└── Configuration
    ├── YAML persistant
    ├── Migration automatique
    └── Registres unifiés
```

## Migration Guide

### Depuis CLI V3
1. Lancer `python setup_cli_v4.py`
2. Configuration automatiquement migrée
3. Nouvelles fonctionnalités disponibles immédiatement

### Depuis CLI V2
1. Réexporter les sessions existantes
2. Utiliser `trading_cli_v4.py init`
3. Reconfigurer avec les nouveaux groupes de commandes

### Depuis CLI de base
1. Sauvegarder les sessions importantes
2. Migration manuelle via interface interactive
3. Bénéficier des nouvelles capacités analytics

## Troubleshooting

### Problèmes courants
- **Workflow non disponible** : Vérifier les imports des modules workflow
- **Multi-session non disponible** : Module optionnel, fonctionnalités réduites
- **Configuration corrompue** : Utiliser `--force` pour réinitialiser

### Support
- Configuration de secours automatique
- Logs détaillés pour debugging
- Mode fallback pour compatibilité maximale

TradingCLI V4 représente l'aboutissement de l'évolution CLI avec toutes les fonctionnalités unifiées dans une interface cohérente et puissante.
