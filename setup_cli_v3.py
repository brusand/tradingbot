#!/usr/bin/env python3
"""
Script d'installation et configuration pour TradingCLI V3
"""

import os
import sys
import yaml
from datetime import datetime, timezone

def create_config_structure():
    """Crée la structure de configuration"""
    config_dir = "config"
    os.makedirs(config_dir, exist_ok=True)
    
    # Configuration par défaut
    default_config = {
        'strategies': {
            'demo_sma_crossover': {
                'id': 'demo_sma_crossover',
                'name': 'Demo SMA Crossover Strategy',
                'symbol': 'BTCUSD',
                'timeframe': '5m',
                'parameters': {
                    'short_window': 10,
                    'long_window': 30
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
                    }
                },
                'signal_rules': {
                    'long_condition': 'SMA_10 > SMA_30 & SMA_10[-1] <= SMA_30[-1] & RSI_14 < 70',
                    'short_condition': 'SMA_10 < SMA_30 & SMA_10[-1] >= SMA_30[-1] & RSI_14 > 30'
                },
                'risk_profile': 'conservative',
                'start_date': '2024-01-01',
                'end_date': 'now',
                'status': 'configured',
                'workflow_enabled': True,
                'created_at': datetime.now(timezone.utc).isoformat()
            },
            'demo_ema_rsi': {
                'id': 'demo_ema_rsi',
                'name': 'Demo EMA RSI Strategy',
                'symbol': 'ETHUSD',
                'timeframe': '15m',
                'parameters': {
                    'short_window': 12,
                    'long_window': 26
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
                    }
                },
                'signal_rules': {
                    'long_condition': 'EMA_12 > EMA_26 & close > EMA_12 & RSI_14 > 50 & RSI_14 < 80',
                    'short_condition': 'EMA_12 < EMA_26 & close < EMA_12 & RSI_14 < 50 & RSI_14 > 20'
                },
                'risk_profile': 'moderate',
                'start_date': '2024-01-01',
                'end_date': 'now',
                'status': 'configured',
                'workflow_enabled': True,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        },
        'indicators': {
            'RSI': {
                'type': 'RSI',
                'description': 'Relative Strength Index',
                'default_parameters': {'period': 14},
                'source': 'close'
            },
            'SMA': {
                'type': 'SMA',
                'description': 'Simple Moving Average',
                'default_parameters': {'period': 20},
                'source': 'close'
            },
            'EMA': {
                'type': 'EMA',
                'description': 'Exponential Moving Average',
                'default_parameters': {'period': 20},
                'source': 'close'
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
            }
        },
        'sessions': {
            'demo_session': {
                'id': 'demo_session',
                'name': 'Session de Démonstration',
                'mode': 'sandbox',
                'workflow_enabled': True,
                'strategies': ['demo_sma_crossover', 'demo_ema_rsi'],
                'status': 'configured',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        }
    }
    
    config_file = os.path.join(config_dir, 'trading_config.yaml')
    with open(config_file, 'w') as f:
        yaml.dump(default_config, f, default_flow_style=False, indent=2)
    
    print(f"✅ Configuration créée: {config_file}")
    return config_file

def create_runner_script():
    """Crée un script de lancement facile"""
    runner_content = '''#!/usr/bin/env python3
"""
Script de lancement pour TradingCLI V3
"""

import sys
import os

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cli.trading_cli_v3 import cli

if __name__ == '__main__':
    cli()
'''
    
    with open('trading_cli.py', 'w') as f:
        f.write(runner_content)
    
    # Rendre exécutable sur Unix
    if os.name != 'nt':
        os.chmod('trading_cli.py', 0o755)
    
    print("✅ Script de lancement créé: trading_cli.py")

def create_quick_start_guide():
    """Crée un guide de démarrage rapide"""
    guide_content = '''# TradingCLI V3 - Guide de Démarrage Rapide

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
'''
    
    with open('QUICKSTART.md', 'w') as f:
        f.write(guide_content)
    
    print("✅ Guide de démarrage créé: QUICKSTART.md")

def main():
    """Installation et configuration"""
    print("🚀 Installation de TradingCLI V3 avec Workflow Avancé")
    print("=" * 60)
    
    # Créer la structure
    config_file = create_config_structure()
    create_runner_script()
    create_quick_start_guide()
    
    print("\n🎉 Installation terminée!")
    print("\n📋 Prochaines étapes:")
    print("1. python trading_cli.py version")
    print("2. python trading_cli.py strategy list")
    print("3. python trading_cli.py system start")
    print("4. python trading_cli.py test workflow --duration 30")
    print("\n📖 Consultez QUICKSTART.md pour un guide complet")

if __name__ == '__main__':
    main()