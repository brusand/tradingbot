#!/usr/bin/env python3
"""
Script de lancement pour l'interface graphique Trading CLI
"""

import sys
import os

# Ajouter le répertoire courant au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from cli.trading_cli_gui import main
    
    if __name__ == "__main__":
        main()
        
except ImportError as e:
    print(f"❌ Erreur d'import: {e}")
    print("💡 Installez les dépendances avec: pip install -r requirements.txt")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erreur: {e}")
    sys.exit(1)