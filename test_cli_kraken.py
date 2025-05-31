#!/usr/bin/env python3
"""
CLI de Test avec Données Kraken
================================

CLI spécialisé pour tester le système de performance avec les données Kraken simulées.
"""

import sys
import os
import click

# Ajouter le chemin du projet
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cli.trading_cli_v4 import TradingCLI, cli as base_cli


# Créer une instance avec la config de test
@click.group()
@click.pass_context
def cli(ctx):
    """🧪 TradingCLI V4 - Test avec Données Kraken"""
    ctx.ensure_object(dict)
    ctx.obj['cli'] = TradingCLI(config_path="config/test_trading_config.yaml")


# Importer toutes les commandes de base mais avec notre config
from cli.trading_cli_v4 import performance, session, strategy, analytics, system, test

# Ajouter les groupes de commandes
cli.add_command(performance)
cli.add_command(session)
cli.add_command(strategy)
cli.add_command(analytics)
cli.add_command(system)
cli.add_command(test)


@cli.command('info')
@click.pass_context
def test_info(ctx):
    """Afficher les informations du test Kraken"""
    trading_cli = ctx.obj['cli']
    
    click.echo("🧪 TEST CONFIGURATION KRAKEN")
    click.echo("=" * 50)
    click.echo(f"📊 Sessions: {len(trading_cli.sessions_registry)}")
    click.echo(f"🎯 Stratégies: {len(trading_cli.strategies_registry)}")
    click.echo(f"📈 Config: test_trading_config.yaml")
    click.echo()
    
    # Résumé des sessions
    click.echo("📋 SESSIONS DE TEST:")
    for session_id, session in trading_cli.sessions_registry.items():
        click.echo(f"  • {session['name']} ({session['mode']})")
        click.echo(f"    - Stratégies: {len(session.get('strategies', []))}")
        if session.get('best_strategy'):
            best = session['best_strategy']
            click.echo(f"    - Meilleure: {best['name']} (${best['pnl']:,.2f})")
    
    click.echo()
    
    # Résumé des stratégies avec données Kraken
    click.echo("🎯 STRATÉGIES KRAKEN:")
    for strategy_id, strategy in trading_cli.strategies_registry.items():
        kraken_data = strategy.get('kraken_data', {})
        if kraken_data:
            pair = kraken_data.get('pair', 'N/A')
            price = kraken_data.get('last_price', 0)
            
            pnl = 0
            if strategy.get('current_balance') and strategy.get('initial_balance'):
                pnl = strategy['current_balance'] - strategy['initial_balance']
            
            pnl_icon = "📈" if pnl >= 0 else "📉"
            click.echo(f"  • {strategy['name']} ({pair})")
            click.echo(f"    - Prix: ${price:,.2f}")
            click.echo(f"    - {pnl_icon} P&L: ${pnl:,.2f}")


if __name__ == '__main__':
    cli()