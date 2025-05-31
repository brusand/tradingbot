#!/usr/bin/env python3
"""
Script pour nettoyer toutes les sessions existantes
"""

import yaml
import os
from datetime import datetime, timezone

def clean_all_sessions():
    config_path = "config/trading_config.yaml"
    
    if not os.path.exists(config_path):
        print("❌ Fichier de configuration introuvable")
        return
    
    # Sauvegarder l'ancien fichier
    backup_path = f"config/trading_config_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
    os.system(f"cp {config_path} {backup_path}")
    print(f"📦 Sauvegarde créée: {backup_path}")
    
    # Charger la configuration actuelle
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Compter les sessions existantes
    sessions_count = len(config.get('sessions', {}))
    print(f"🗑️ {sessions_count} sessions trouvées")
    
    # Vider les sessions
    config['sessions'] = {}
    
    # Sauvegarder
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)
    
    print(f"✅ Toutes les sessions ont été supprimées")
    print(f"💾 Stratégies et profils de risque conservés")
    
    return True

if __name__ == '__main__':
    clean_all_sessions()