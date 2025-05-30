#!/usr/bin/env python3
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
