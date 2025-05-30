#!/usr/bin/env python3
"""
Script de lancement pour TradingCLI V3
"""

import sys
import os

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from interfaces.cli import cli

if __name__ == '__main__':
    cli()
