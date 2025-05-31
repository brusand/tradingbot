#!/usr/bin/env python3
"""
Script de migration et configuration pour TradingCLI V4
Fusion complète de toutes les versions CLI
"""

import os
import sys
import yaml
import shutil
from datetime import datetime, timezone

def backup_existing_config():
    """Sauvegarde les configurations existantes"""
    config_files = [
        "config/trading_config.yaml",
        "interfaces/cli.py",
        "interfaces/cli_v2.py", 
        "interfaces/cli_adapted.py",
        "cli/trading_cli_v3.py"
    ]
    
    backup_dir = f"backup_cli_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(backup_dir, exist_ok=True)
    
    backed_up = []
    for config_file in config_files:
        if os.path.exists(config_file):
            backup_path = os.path.join(backup_dir, os.path.basename(config_file))
            shutil.copy2(config_file, backup_path)
            backed_up.append(config_file)
    
    if backed_up:
        print(f"📦 Sauvegarde créée: {backup_dir}")
        for file in backed_up:
            print(f"  - {file}")
    
    return backup_dir

def migrate_existing_config():
    """Migre la configuration existante vers V4"""
    existing_config = {}
    
    # Lire la config V3 si elle existe
    v3_config_path = "config/trading_config.yaml"
    if os.path.exists(v3_config_path):
        try:
            with open(v3_config_path, 'r') as f:
                existing_config = yaml.safe_load(f) or {}
                print(f"✅ Configuration V3 trouvée et chargée")
        except Exception as e:
            print(f"⚠️ Erreur lecture config V3: {e}")
    
    return existing_config

def create_v4_config(existing_config=None):
    """Crée la configuration V4 unifiée"""
    
    # Configuration de base V4
    v4_config = {
        'strategies': {},
        'indicators': {
            'RSI': {
                'type': 'RSI',
                'description': 'Relative Strength Index',
                'default_parameters': {'period': 14},
                'source': 'close',
                'workflow_support': True
            },
            'SMA': {
                'type': 'SMA',
                'description': 'Simple Moving Average',
                'default_parameters': {'period': 20},
                'source': 'close',
                'workflow_support': True
            },
            'EMA': {
                'type': 'EMA',
                'description': 'Exponential Moving Average',
                'default_parameters': {'period': 20},
                'source': 'close',
                'workflow_support': True
            },
            'MACD': {
                'type': 'MACD',
                'description': 'Moving Average Convergence Divergence',
                'default_parameters': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
                'source': 'close',
                'workflow_support': True
            }
        },
        'risk_profiles': {
            'conservative': {
                'name': 'Profil Conservateur',
                'position_size_method': 'percent',
                'position_size_value': 1.0,
                'max_concurrent_trades': 2,
                'max_daily_loss': 200.0,
                'max_drawdown_percent': 10.0,
                'stop_loss_percent': 1.5,
                'take_profit_percent': 3.0,
                'trailing_stop': False
            },
            'moderate': {
                'name': 'Profil Modéré',
                'position_size_method': 'percent',
                'position_size_value': 2.0,
                'max_concurrent_trades': 3,
                'max_daily_loss': 500.0,
                'max_drawdown_percent': 15.0,
                'stop_loss_percent': 2.0,
                'take_profit_percent': 4.0,
                'trailing_stop': True
            },
            'aggressive': {
                'name': 'Profil Agressif',
                'position_size_method': 'percent',
                'position_size_value': 5.0,
                'max_concurrent_trades': 5,
                'max_daily_loss': 1000.0,
                'max_drawdown_percent': 25.0,
                'stop_loss_percent': 3.0,
                'take_profit_percent': 6.0,
                'trailing_stop': True
            },
            'default': {
                'name': 'Profil par défaut',
                'position_size_method': 'percent',
                'position_size_value': 2.0,
                'max_concurrent_trades': 3,
                'max_daily_loss': 500.0,
                'max_drawdown_percent': 20.0,
                'stop_loss_percent': 2.0,
                'take_profit_percent': 4.0,
                'trailing_stop': False
            }
        },
        'sessions': {}
    }
    
    # Migrer les données existantes si présentes
    if existing_config:
        print("🔄 Migration des données existantes...")
        
        # Migrer stratégies
        if 'strategies' in existing_config:
            for strategy_id, strategy_data in existing_config['strategies'].items():
                # Enrichir la stratégie avec les nouvelles capacités V4
                migrated_strategy = {
                    **strategy_data,
                    'migrated_from': 'V3',
                    'migration_date': datetime.now(timezone.utc).isoformat(),
                    'v4_features': {
                        'multi_session_compatible': True,
                        'analytics_enabled': True,
                        'workflow_enhanced': strategy_data.get('workflow_enabled', False)
                    }
                }
                v4_config['strategies'][strategy_id] = migrated_strategy
                print(f"  ✅ Stratégie migrée: {strategy_id}")
        
        # Migrer sessions
        if 'sessions' in existing_config:
            for session_id, session_data in existing_config['sessions'].items():
                migrated_session = {
                    **session_data,
                    'migrated_from': 'V3',
                    'migration_date': datetime.now(timezone.utc).isoformat(),
                    'v4_features': {
                        'analytics_enabled': True,
                        'multi_session_compatible': True,
                        'portfolio_tracking': True
                    }
                }
                v4_config['sessions'][session_id] = migrated_session
                print(f"  ✅ Session migrée: {session_id}")
        
        # Migrer profils de risque
        if 'risk_profiles' in existing_config:
            for profile_id, profile_data in existing_config['risk_profiles'].items():
                if profile_id not in v4_config['risk_profiles']:
                    v4_config['risk_profiles'][profile_id] = profile_data
                    print(f"  ✅ Profil de risque migré: {profile_id}")
    
    # Ajouter des exemples V4 si pas de données existantes
    if not v4_config['strategies']:
        print("📝 Création d'exemples de stratégies V4...")
        
        v4_config['strategies'].update({
            'v4_sma_crossover': {
                'id': 'v4_sma_crossover',
                'name': 'V4 SMA Crossover Strategy',
                'symbol': 'BTCUSD',
                'timeframe': '5m',
                'parameters': {
                    'short_window': 10,
                    'long_window': 30,
                    'risk_management': True
                },
                'indicators': {
                    'SMA_10': {
                        'type': 'SMA',
                        'parameters': {'period': 10},
                        'source': 'close'
                    },
                    'SMA_30': {
                        'type': 'SMA',
                        'parameters': {'period': 30},
                        'source': 'close'
                    },
                    'RSI_14': {
                        'type': 'RSI',
                        'parameters': {'period': 14},
                        'source': 'close'
                    },
                    'MACD': {
                        'type': 'MACD',
                        'parameters': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
                        'source': 'close'
                    }
                },
                'signal_rules': {
                    'long_condition': 'SMA_10 > SMA_30 & SMA_10[-1] <= SMA_30[-1] & RSI_14 < 70 & RSI_14 > 30',
                    'short_condition': 'SMA_10 < SMA_30 & SMA_10[-1] >= SMA_30[-1] & RSI_14 > 70'
                },
                'risk_profile': 'moderate',
                'start_date': '2024-01-01',
                'end_date': 'now',
                'status': 'configured',
                'workflow_enabled': True,
                'v4_features': {
                    'analytics_enabled': True,
                    'multi_session_compatible': True,
                    'advanced_workflow': True
                },
                'created_at': datetime.now(timezone.utc).isoformat()
            },
            'v4_ema_momentum': {
                'id': 'v4_ema_momentum',
                'name': 'V4 EMA Momentum Strategy',
                'symbol': 'ETHUSD',
                'timeframe': '15m',
                'parameters': {
                    'fast_ema': 12,
                    'slow_ema': 26,
                    'momentum_threshold': 0.02
                },
                'indicators': {
                    'EMA_12': {
                        'type': 'EMA',
                        'parameters': {'period': 12},
                        'source': 'close'
                    },
                    'EMA_26': {
                        'type': 'EMA',
                        'parameters': {'period': 26},
                        'source': 'close'
                    },
                    'RSI_14': {
                        'type': 'RSI',
                        'parameters': {'period': 14},
                        'source': 'close'
                    },
                    'MACD': {
                        'type': 'MACD',
                        'parameters': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
                        'source': 'close'
                    }
                },
                'signal_rules': {
                    'long_condition': 'EMA_12 > EMA_26 & close > EMA_12 & RSI_14 > 50 & RSI_14 < 80',
                    'short_condition': 'EMA_12 < EMA_26 & close < EMA_12 & RSI_14 < 50 & RSI_14 > 20'
                },
                'risk_profile': 'aggressive',
                'start_date': '2024-01-01',
                'end_date': 'now',
                'status': 'configured',
                'workflow_enabled': True,
                'v4_features': {
                    'analytics_enabled': True,
                    'multi_session_compatible': True,
                    'advanced_workflow': True
                },
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        })
    
    if not v4_config['sessions']:
        print("📝 Création d'exemples de sessions V4...")
        
        v4_config['sessions'].update({
            'v4_demo_portfolio': {
                'id': 'v4_demo_portfolio',
                'name': 'V4 Demo Portfolio Session',
                'mode': 'sandbox',
                'workflow_enabled': True,
                'strategies': ['v4_sma_crossover', 'v4_ema_momentum'],
                'initial_balance': 10000.0,
                'status': 'configured',
                'v4_features': {
                    'portfolio_analytics': True,
                    'multi_strategy': True,
                    'risk_management': True,
                    'real_time_monitoring': True
                },
                'created_at': datetime.now(timezone.utc).isoformat()
            },
            'v4_conservative_session': {
                'id': 'v4_conservative_session',
                'name': 'V4 Conservative Trading Session',
                'mode': 'paper',
                'workflow_enabled': True,
                'strategies': ['v4_sma_crossover'],
                'initial_balance': 5000.0,
                'status': 'configured',
                'risk_profile': 'conservative',
                'v4_features': {
                    'conservative_risk': True,
                    'paper_trading': True,
                    'analytics_enabled': True
                },
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        })
    
    return v4_config

def create_v4_runner():
    """Crée le script de lancement V4"""
    runner_content = '''#!/usr/bin/env python3
"""
TradingCLI V4 - Script de lancement unifié
Fusion complète de toutes les versions CLI
"""

import sys
import os

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cli.trading_cli_v4 import cli

if __name__ == '__main__':
    cli()
'''
    
    with open('trading_cli_v4.py', 'w') as f:
        f.write(runner_content)
    
    # Rendre exécutable sur Unix
    if os.name != 'nt':
        os.chmod('trading_cli_v4.py', 0o755)
    
    print("✅ Script de lancement V4 créé: trading_cli_v4.py")

def create_v4_documentation():
    """Crée la documentation V4"""
    doc_content = '''# TradingCLI V4 - Documentation Complète

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
'''
    
    with open('TRADINGCLI_V4_GUIDE.md', 'w') as f:
        f.write(doc_content)
    
    print("✅ Documentation V4 créée: TRADINGCLI_V4_GUIDE.md")

def main():
    """Migration et configuration V4"""
    print("🚀 MIGRATION VERS TRADINGCLI V4")
    print("=" * 60)
    print("Fusion de toutes les versions CLI existantes")
    print()
    
    # 1. Sauvegarde
    backup_dir = backup_existing_config()
    
    # 2. Migration
    existing_config = migrate_existing_config()
    
    # 3. Création config V4
    print("\n📝 Création de la configuration V4...")
    v4_config = create_v4_config(existing_config)
    
    # 4. Sauvegarde config V4
    config_dir = "config"
    os.makedirs(config_dir, exist_ok=True)
    config_file = os.path.join(config_dir, 'trading_config.yaml')
    
    with open(config_file, 'w') as f:
        yaml.dump(v4_config, f, default_flow_style=False, indent=2)
    
    print(f"✅ Configuration V4 sauvegardée: {config_file}")
    
    # 5. Scripts et documentation
    create_v4_runner()
    create_v4_documentation()
    
    print("\n🎉 MIGRATION V4 TERMINÉE!")
    print("=" * 60)
    print("📊 Résumé:")
    print(f"  Stratégies migrées: {len(v4_config['strategies'])}")
    print(f"  Sessions migrées: {len(v4_config['sessions'])}")
    print(f"  Profils de risque: {len(v4_config['risk_profiles'])}")
    
    print("\n🚀 Prochaines étapes:")
    print("1. python trading_cli_v4.py version")
    print("2. python trading_cli_v4.py system start --mode workflow")
    print("3. python trading_cli_v4.py session list")
    print("4. python trading_cli_v4.py test workflow --duration 30")
    print("5. python trading_cli_v4.py analytics portfolio")
    
    print(f"\n📖 Documentation complète: TRADINGCLI_V4_GUIDE.md")
    
    if backup_dir:
        print(f"📦 Sauvegarde des anciennes versions: {backup_dir}")

if __name__ == '__main__':
    main()