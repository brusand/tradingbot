#!/usr/bin/env python3
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
