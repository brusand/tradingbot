import click
import asyncio
import json
import yaml
from typing import List, Dict, Optional
from datetime import datetime, timezone
from tabulate import tabulate
import os

from data.models import SessionMode, StrategyConfig, RiskConfig
from data.persistence import DatabaseManager
from core.session_manager import SessionManager
from strategies.performance_tracker import PerformanceTracker


class TradingCLI:
    def __init__(self, config_path: str = "config/trading_config.yaml"):
        self.config_path = config_path
        self.session_manager = None
        self.db_manager = DatabaseManager()
        
        # Registres basés sur le prompt
        self.strategies_registry = {}
        self.indicators_registry = {}
        self.risk_profiles_registry = {}
        self.sessions_registry = {}
        
        # Charger configuration
        self._load_configuration()
        
        # Initialiser le session manager pour la compatibilité
        self.session_manager = SessionManager(self.db_manager)
    
    def _load_configuration(self):
        """Charge la configuration depuis les fichiers"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.strategies_registry = config.get('strategies', {})
                self.indicators_registry = config.get('indicators', {})
                self.risk_profiles_registry = config.get('risk_profiles', {})
                self.sessions_registry = config.get('sessions', {})
    
    def _save_configuration(self):
        """Sauvegarde la configuration"""
        config = {
            'strategies': self.strategies_registry,
            'indicators': self.indicators_registry,
            'risk_profiles': self.risk_profiles_registry,
            'sessions': self.sessions_registry
        }
        
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)
    
    async def initialize(self):
        """Initialise les composants async"""
        await self.session_manager.initialize()


@click.group()
@click.pass_context
def cli(ctx):
    """Trading CLI - Configurateur de Sessions de Trading"""
    ctx.ensure_object(dict)
    ctx.obj['cli'] = TradingCLI()


# ============================================================================
# COMMANDES SESSIONS ADAPTÉES AU PROMPT
# ============================================================================

@cli.group()
def session():
    """Gestion des sessions de trading"""
    pass


@session.command('create')
@click.option('--name', prompt='Nom de la session', help='Nom de la session')
@click.option('--mode', 
              type=click.Choice(['sandbox', 'paper', 'live']),
              prompt='Mode de trading',
              help='Mode de trading')
@click.pass_context
def create_session(ctx, name, mode):
    """Créer une nouvelle session de trading"""
    trading_cli = ctx.obj['cli']
    
    session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    session_config = {
        'id': session_id,
        'name': name,
        'mode': mode,
        'strategies': [],
        'status': 'created',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'start_time': None,
        'end_time': None,
        'performance': {
            'initial_balance': 10000,
            'current_balance': 10000,
            'total_pnl': 0.0,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'profit_factor': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0
        }
    }
    
    trading_cli.sessions_registry[session_id] = session_config
    trading_cli._save_configuration()
    
    click.echo(f"✅ Session '{name}' créée avec l'ID '{session_id}'")
    click.echo(f"💡 Utilisez 'session add-strategy {session_id} <strategy_id>' pour ajouter des stratégies")


@session.command('list')
@click.pass_context
def list_sessions(ctx):
    """Lister toutes les sessions"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.sessions_registry:
        click.echo("Aucune session configurée.")
        return
    
    table_data = []
    for session_id, session in trading_cli.sessions_registry.items():
        # Calcul de métriques de performance
        perf = session.get('performance', {})
        total_pnl = perf.get('total_pnl', 0)
        total_trades = perf.get('total_trades', 0)
        win_rate = perf.get('win_rate', 0)
        
        table_data.append([
            session_id[:12] + "..." if len(session_id) > 12 else session_id,
            session['name'][:20] if len(session['name']) > 20 else session['name'],
            session['mode'],
            len(session.get('strategies', [])),
            session.get('status', 'unknown'),
            f"{total_pnl:.2f}",
            total_trades,
            f"{win_rate:.1%}" if win_rate > 0 else "0.0%",
            session.get('created_at', 'N/A')[:10]  # Date seulement
        ])
    
    headers = ['ID', 'Nom', 'Mode', 'Stratégies', 'Status', 'PnL', 'Trades', 'Win Rate', 'Créé le']
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))


@session.command('show')
@click.argument('session_id')
@click.option('--detailed', is_flag=True, help='Afficher les métriques de performance détaillées')
@click.option('--trades', is_flag=True, help='Afficher l'historique des trades')
@click.option('--export', type=click.Path(), help='Exporter le rapport vers un fichier JSON')
@click.pass_context
def show_session(ctx, session_id, detailed, trades, export):
    """Afficher les détails d'une session avec performances"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable!")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    # Header stylisé
    click.echo("=" * 80)
    click.echo(f"📊 SESSION DETAILS: {session['name']}")
    click.echo("=" * 80)
    
    # Informations de base avec émojis
    click.echo(f"🆔 ID: {session_id}")
    click.echo(f"📝 Name: {session['name']}")
    click.echo(f"🎯 Mode: {session['mode']}")
    
    # Status avec couleurs
    status_emoji = {
        'created': '⚪',
        'running': '🟢',
        'active': '🟢', 
        'paused': '🟡',
        'stopped': '🔴',
        'error': '❌'
    }
    status_icon = status_emoji.get(session.get('status', 'unknown').lower(), '❓')
    click.echo(f"{status_icon} Status: {session.get('status', 'unknown')}")
    
    click.echo(f"📅 Created: {session.get('created_at', 'N/A')}")
    if session.get('start_time'):
        click.echo(f"🚀 Started: {session.get('start_time', 'N/A')}")
    if session.get('end_time'):
        click.echo(f"🏁 Ended: {session.get('end_time', 'N/A')}")
    
    # Stratégies associées
    strategies = session.get('strategies', [])
    click.echo(f"\n🎯 ASSOCIATED STRATEGIES ({len(strategies)}):")
    if strategies:
        for strategy_id in strategies:
            if strategy_id in trading_cli.strategies_registry:
                strategy = trading_cli.strategies_registry[strategy_id]
                click.echo(f"  📈 {strategy.get('name', strategy_id)} ({strategy.get('symbol', 'N/A')}, {strategy.get('timeframe', 'N/A')})")
            else:
                click.echo(f"  ⚠️ {strategy_id} (configuration manquante)")
    else:
        click.echo("  Aucune stratégie assignée")
    
    # Performance de base
    perf = session.get('performance', {})
    click.echo(f"\n💰 BASIC PERFORMANCE:")
    
    total_pnl = perf.get('total_pnl', 0)
    pnl_icon = "🟢" if total_pnl >= 0 else "🔴"
    click.echo(f"  {pnl_icon} Total PnL: {total_pnl:.2f}")
    
    current_balance = perf.get('current_balance', 10000)
    initial_balance = perf.get('initial_balance', 10000)
    roi = ((current_balance - initial_balance) / initial_balance * 100) if initial_balance > 0 else 0
    click.echo(f"  💵 Current Balance: {current_balance:.2f}")
    click.echo(f"  📈 ROI: {roi:.2f}%")
    
    total_trades = perf.get('total_trades', 0)
    click.echo(f"  📊 Total Trades: {total_trades}")
    
    win_rate = perf.get('win_rate', 0)
    win_rate_icon = "🎯" if win_rate >= 0.6 else "📈" if win_rate >= 0.4 else "📉"
    click.echo(f"  {win_rate_icon} Win Rate: {win_rate:.1%}")
    
    # Métriques détaillées si demandées
    if detailed:
        click.echo(f"\n📊 ADVANCED PERFORMANCE METRICS:")
        
        # Métriques de risque
        max_drawdown = perf.get('max_drawdown', 0)
        click.echo(f"  📉 Max Drawdown: {max_drawdown:.2f}%")
        
        sharpe_ratio = perf.get('sharpe_ratio', 0)
        click.echo(f"  🎯 Sharpe Ratio: {sharpe_ratio:.3f}")
        
        profit_factor = perf.get('profit_factor', 0)
        click.echo(f"  📊 Profit Factor: {profit_factor:.2f}")
        
        # Statistiques des trades
        winning_trades = perf.get('winning_trades', 0)
        losing_trades = perf.get('losing_trades', 0)
        click.echo(f"  🏆 Winning Trades: {winning_trades}")
        click.echo(f"  💔 Losing Trades: {losing_trades}")
        
        avg_win = perf.get('avg_win', 0)
        avg_loss = perf.get('avg_loss', 0)
        click.echo(f"  📊 Avg Win: {avg_win:.2f}")
        click.echo(f"  📉 Avg Loss: {avg_loss:.2f}")
        
        # Métriques de consistance
        max_consecutive_wins = perf.get('max_consecutive_wins', 0)
        max_consecutive_losses = perf.get('max_consecutive_losses', 0)
        click.echo(f"  🔥 Max Consecutive Wins: {max_consecutive_wins}")
        click.echo(f"  ❄️ Max Consecutive Losses: {max_consecutive_losses}")
        
        # Informations temporelles
        if perf.get('start_date') and perf.get('end_date'):
            click.echo(f"  ⏱️ Trading Period: {perf.get('start_date')} → {perf.get('end_date')}")
        
        volatility = perf.get('volatility', 0)
        if volatility > 0:
            click.echo(f"  📊 Volatility: {volatility:.2f}%")
        
        var_95 = perf.get('var_95', 0)
        if var_95 != 0:
            click.echo(f"  ⚠️ VaR (95%): {var_95:.2f}")
    
    # Historique des trades si demandé
    if trades:
        click.echo(f"\n📋 RECENT TRADES:")
        
        # Simuler quelques trades pour la démonstration
        # Dans une vraie implémentation, on récupérerait depuis la DB
        recent_trades = perf.get('recent_trades', [])
        
        if recent_trades:
            click.echo(f"{'Date':<20} {'Side':<6} {'Symbol':<10} {'Amount':<12} {'Price':<12} {'PnL':<12}")
            click.echo("-" * 80)
            
            for trade in recent_trades[-10:]:  # 10 derniers trades
                trade_pnl = trade.get('pnl', 0)
                pnl_icon = "🟢" if trade_pnl >= 0 else "🔴"
                
                click.echo(f"{trade.get('date', 'N/A'):<20} "
                          f"{trade.get('side', 'N/A'):<6} "
                          f"{trade.get('symbol', 'N/A'):<10} "
                          f"{trade.get('amount', 0):<12.4f} "
                          f"{trade.get('price', 0):<12.2f} "
                          f"{pnl_icon} {trade_pnl:<9.2f}")
        else:
            click.echo("  Aucun trade disponible")
            click.echo("  💡 Les trades s'afficheront une fois la session démarrée")
    
    # Configuration des stratégies détaillée
    if detailed and strategies:
        click.echo(f"\n🎯 STRATEGIES CONFIGURATION:")
        for strategy_id in strategies:
            if strategy_id in trading_cli.strategies_registry:
                strategy = trading_cli.strategies_registry[strategy_id]
                click.echo(f"\n📈 {strategy.get('name', strategy_id)}:")
                click.echo(f"  💰 Symbol: {strategy.get('symbol', 'Non défini')}")
                click.echo(f"  ⏰ Timeframe: {strategy.get('timeframe', 'Non défini')}")
                
                # Risk profile
                risk_profile_id = strategy.get('risk_profile', 'default')
                if risk_profile_id in trading_cli.risk_profiles_registry:
                    risk_profile = trading_cli.risk_profiles_registry[risk_profile_id]
                    click.echo(f"  🛡️ Risk Profile: {risk_profile.get('name', risk_profile_id)}")
                    click.echo(f"    - Position Size: {risk_profile.get('position_size_value', 0)}%")
                    click.echo(f"    - Stop Loss: {risk_profile.get('stop_loss_percent', 0)}%")
                    click.echo(f"    - Take Profit: {risk_profile.get('take_profit_percent', 0)}%")
                
                # Indicateurs
                indicators = strategy.get('indicators', {})
                if indicators:
                    click.echo(f"  🔧 Indicators ({len(indicators)}):")
                    for ind_name, ind_config in indicators.items():
                        params = ind_config.get('parameters', {})
                        params_str = ', '.join([f"{k}={v}" for k, v in params.items()])
                        click.echo(f"    - {ind_name} ({ind_config.get('type', 'N/A')}): {params_str}")
                
                # Conditions de signal
                signals = strategy.get('signal_rules', {})
                if signals.get('long_condition'):
                    click.echo(f"  📈 Long Signal: {signals['long_condition']}")
                if signals.get('short_condition'):
                    click.echo(f"  📉 Short Signal: {signals['short_condition']}")
    
    # Export si demandé
    if export:
        try:
            # Préparer les données d'export enrichies
            export_data = {
                'session_id': session_id,
                'session_name': session['name'],
                'mode': session['mode'],
                'status': session.get('status', 'unknown'),
                'created_at': session.get('created_at'),
                'start_time': session.get('start_time'),
                'end_time': session.get('end_time'),
                'strategies': [],
                'performance': perf,
                'exported_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Ajouter les détails des stratégies
            for strategy_id in strategies:
                if strategy_id in trading_cli.strategies_registry:
                    strategy = trading_cli.strategies_registry[strategy_id]
                    export_data['strategies'].append({
                        'id': strategy_id,
                        'name': strategy.get('name'),
                        'symbol': strategy.get('symbol'),
                        'timeframe': strategy.get('timeframe'),
                        'indicators': strategy.get('indicators', {}),
                        'signal_rules': strategy.get('signal_rules', {}),
                        'risk_profile': strategy.get('risk_profile')
                    })
            
            with open(export, 'w') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            click.echo(f"\n💾 Rapport détaillé exporté vers: {export}")
            click.echo(f"📊 Contient: session, stratégies, performances, et configuration complète")
            
        except Exception as e:
            click.echo(f"\n❌ Erreur lors de l'export: {e}")
    
    # Footer avec conseils
    click.echo("\n" + "=" * 80)
    click.echo("💡 Options disponibles:")
    click.echo("  --detailed : Métriques avancées et configuration des stratégies")
    click.echo("  --trades   : Historique des trades récents")
    click.echo("  --export   : Export JSON complet pour analyse externe")
    click.echo("🚀 Utilisez 'session start <session_id>' pour démarrer la session")


@session.command('start')
@click.argument('session_id')
@click.pass_context
def start_session(ctx, session_id):
    """Démarrer une session de trading"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable!")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    # Vérifier que toutes les stratégies sont configurées
    missing_config = []
    strategies = session.get('strategies', [])
    
    if not strategies:
        missing_config.append("Aucune stratégie assignée à la session")
    
    for strategy_id in strategies:
        if strategy_id not in trading_cli.strategies_registry:
            missing_config.append(f"Stratégie '{strategy_id}' introuvable")
            continue
        
        strategy = trading_cli.strategies_registry[strategy_id]
        
        # Vérifications de configuration
        if not strategy.get('symbol'):
            missing_config.append(f"Stratégie '{strategy_id}': paire non définie")
        if not strategy.get('timeframe'):
            missing_config.append(f"Stratégie '{strategy_id}': timeframe non défini")
        if not strategy.get('indicators'):
            missing_config.append(f"Stratégie '{strategy_id}': aucun indicateur configuré")
        if not strategy.get('signal_rules', {}).get('long_condition') and not strategy.get('signal_rules', {}).get('short_condition'):
            missing_config.append(f"Stratégie '{strategy_id}': aucune condition de signal définie")
    
    if missing_config:
        click.echo("❌ Configuration incomplète:")
        for error in missing_config:
            click.echo(f"  - {error}")
        click.echo("\n💡 Utilisez les commandes 'strategy configure' et 'indicator add' pour compléter")
        return
    
    # Initialiser les performances si nécessaire
    if 'performance' not in session:
        session['performance'] = {
            'initial_balance': 10000,
            'current_balance': 10000,
            'total_pnl': 0.0,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'profit_factor': 0.0,
            'start_date': datetime.now().strftime('%Y-%m-%d'),
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
    
    # Démarrer la session
    session['status'] = 'running'
    session['start_time'] = datetime.now(timezone.utc).isoformat()
    
    # Marquer les stratégies comme actives
    for strategy_id in strategies:
        if strategy_id in trading_cli.strategies_registry:
            trading_cli.strategies_registry[strategy_id]['status'] = 'active'
    
    trading_cli._save_configuration()
    
    click.echo(f"🚀 Session '{session['name']}' démarrée avec succès!")
    click.echo(f"📊 Mode: {session['mode']}")
    click.echo(f"🎯 {len(strategies)} stratégie(s) active(s)")
    
    # Afficher résumé des stratégies
    click.echo("\n📈 Stratégies actives:")
    for strategy_id in strategies:
        if strategy_id in trading_cli.strategies_registry:
            strategy = trading_cli.strategies_registry[strategy_id]
            click.echo(f"  - {strategy.get('name', strategy_id)} ({strategy.get('symbol', 'N/A')}, {strategy.get('timeframe', 'N/A')})")
    
    click.echo(f"\n💡 Utilisez 'session show {session_id} --detailed' pour suivre les performances")


@session.command('stop')
@click.argument('session_id')
@click.pass_context
def stop_session(ctx, session_id):
    """Arrêter une session de trading"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable!")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    # Arrêter la session
    session['status'] = 'stopped'
    session['end_time'] = datetime.now(timezone.utc).isoformat()
    
    # Marquer les stratégies comme inactives
    for strategy_id in session.get('strategies', []):
        if strategy_id in trading_cli.strategies_registry:
            trading_cli.strategies_registry[strategy_id]['status'] = 'inactive'
    
    trading_cli._save_configuration()
    
    click.echo(f"🛑 Session '{session['name']}' arrêtée")
    click.echo(f"📊 Durée totale: {session.get('start_time', 'N/A')} → {session.get('end_time', 'N/A')}")


@session.command('add-strategy')
@click.argument('session_id')
@click.argument('strategy_id')
@click.pass_context
def add_strategy_to_session(ctx, session_id, strategy_id):
    """Ajouter une stratégie à une session"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable!")
        return
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    if 'strategies' not in session:
        session['strategies'] = []
    
    if strategy_id not in session['strategies']:
        session['strategies'].append(strategy_id)
        trading_cli._save_configuration()
        click.echo(f"✅ Stratégie '{strategy_id}' ajoutée à la session '{session_id}'")
    else:
        click.echo(f"⚠️ Stratégie '{strategy_id}' déjà dans la session!")


# ============================================================================
# COMMANDES PERFORMANCE (ADAPTÉES DU CLI EXISTANT)
# ============================================================================

@cli.command()
@click.option('--limit', default=10, type=int, help='Number of sessions to show')
@click.option('--sort-by', type=click.Choice(['pnl', 'trades', 'win_rate', 'created']), 
              default='pnl', help='Sort criterion')
@click.pass_context
def performance(ctx, limit, sort_by):
    """Show performance summary of all sessions"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.sessions_registry:
        click.echo("Aucune session trouvée.")
        return
    
    # Préparer les données de performance
    performance_data = []
    for session_id, session in trading_cli.sessions_registry.items():
        perf = session.get('performance', {})
        
        performance_data.append({
            'id': session_id[:8] + "...",  # Truncate ID for display
            'name': session['name'][:20],     # Truncate name
            'mode': session['mode'],
            'status': session.get('status', 'unknown'),
            'pnl': perf.get('total_pnl', 0),
            'trades': perf.get('total_trades', 0),
            'win_rate': perf.get('win_rate', 0),
            'created': session.get('created_at', ''),
            'strategies_count': len(session.get('strategies', []))
        })
    
    # Trier selon le critère
    if sort_by == 'pnl':
        performance_data.sort(key=lambda x: x['pnl'], reverse=True)
    elif sort_by == 'trades':
        performance_data.sort(key=lambda x: x['trades'], reverse=True)
    elif sort_by == 'win_rate':
        performance_data.sort(key=lambda x: x['win_rate'], reverse=True)
    elif sort_by == 'created':
        performance_data.sort(key=lambda x: x['created'], reverse=True)
    
    # Limiter les résultats
    performance_data = performance_data[:limit]
    
    # Affichage
    click.echo(f"\n🏆 TOP {limit} SESSIONS (sorted by {sort_by}):")
    click.echo("=" * 100)
    click.echo(f"{'ID':<12} {'Name':<22} {'Mode':<8} {'Status':<10} {'Strategies':<12} {'PnL':<12} {'Trades':<8} {'Win Rate':<10}")
    click.echo("-" * 100)
    
    for data in performance_data:
        # Icons pour PnL
        pnl_icon = "🟢" if data['pnl'] >= 0 else "🔴"
        pnl_str = f"{pnl_icon} {data['pnl']:.2f}"
        
        # Icons pour status
        status_icons = {'running': '🟢', 'active': '🟢', 'paused': '🟡', 'stopped': '🔴', 'created': '⚪'}
        status_icon = status_icons.get(data['status'], '❓')
        status_str = f"{status_icon} {data['status'][:8]}"
        
        # Win rate avec icon
        win_rate_icon = "🎯" if data['win_rate'] >= 0.6 else "📈" if data['win_rate'] >= 0.4 else "📉"
        win_rate_str = f"{win_rate_icon} {data['win_rate']:.1%}"
        
        click.echo(f"{data['id']:<12} {data['name']:<22} {data['mode']:<8} "
                  f"{status_str:<13} {data['strategies_count']:<12} {pnl_str:<15} "
                  f"{data['trades']:<8} {win_rate_str:<13}")
    
    click.echo("\n💡 Use 'session show <session_id> --detailed' for comprehensive metrics")


@cli.command()
@click.argument('session_ids', nargs=-1, required=True)
@click.pass_context
def compare(ctx, session_ids):
    """Compare performance between multiple sessions"""
    trading_cli = ctx.obj['cli']
    
    sessions_data = []
    
    for session_id in session_ids:
        if session_id not in trading_cli.sessions_registry:
            click.echo(f"⚠️ Session '{session_id}' not found, skipping")
            continue
        
        session = trading_cli.sessions_registry[session_id]
        perf = session.get('performance', {})
        
        sessions_data.append({
            'id': session_id,
            'name': session['name'],
            'pnl': perf.get('total_pnl', 0),
            'trades': perf.get('total_trades', 0),
            'win_rate': perf.get('win_rate', 0),
            'mode': session['mode'],
            'strategies_count': len(session.get('strategies', []))
        })
    
    if len(sessions_data) < 2:
        click.echo("❌ Need at least 2 valid sessions to compare")
        return
    
    click.echo(f"\n📊 COMPARISON OF {len(sessions_data)} SESSIONS:")
    click.echo("=" * 80)
    
    # Tableau de comparaison
    click.echo(f"{'Session':<25} {'Mode':<8} {'Strategies':<12} {'PnL':<12} {'Trades':<8} {'Win Rate':<10}")
    click.echo("-" * 80)
    
    for data in sessions_data:
        pnl_icon = "🟢" if data['pnl'] >= 0 else "🔴"
        win_rate_icon = "🎯" if data['win_rate'] >= 0.6 else "📈" if data['win_rate'] >= 0.4 else "📉"
        
        session_display = f"{data['name'][:20]} ({data['mode']})"
        
        click.echo(f"{session_display:<25} {data['mode']:<8} {data['strategies_count']:<12} "
                  f"{pnl_icon} {data['pnl']:<9.2f} {data['trades']:<8} "
                  f"{win_rate_icon} {data['win_rate']:<7.1%}")
    
    # Champions par catégorie
    click.echo(f"\n🏆 BEST IN CATEGORY:")
    
    best_pnl = max(sessions_data, key=lambda x: x['pnl'])
    click.echo(f"  💰 Best PnL: {best_pnl['name']} ({best_pnl['pnl']:.2f})")
    
    best_trades = max(sessions_data, key=lambda x: x['trades'])
    click.echo(f"  📊 Most Trades: {best_trades['name']} ({best_trades['trades']} trades)")
    
    best_win_rate = max(sessions_data, key=lambda x: x['win_rate'])
    click.echo(f"  🎯 Best Win Rate: {best_win_rate['name']} ({best_win_rate['win_rate']:.1%})")
    
    # Statistiques globales
    total_pnl = sum(s['pnl'] for s in sessions_data)
    total_trades = sum(s['trades'] for s in sessions_data)
    avg_win_rate = sum(s['win_rate'] for s in sessions_data) / len(sessions_data)
    
    click.echo(f"\n📈 AGGREGATE STATS:")
    click.echo(f"  💰 Total PnL: {total_pnl:.2f}")
    click.echo(f"  📊 Total Trades: {total_trades}")
    click.echo(f"  🎯 Average Win Rate: {avg_win_rate:.1%}")


@cli.command()
@click.pass_context
def status(ctx):
    """Show status of all sessions"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.sessions_registry:
        click.echo("Aucune session configurée.")
        return
    
    # Compter les sessions par status
    status_counts = {}
    running_sessions = []
    
    for session_id, session in trading_cli.sessions_registry.items():
        status = session.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
        
        if status == 'running':
            running_sessions.append(session)
    
    click.echo("📊 SYSTEM STATUS:")
    click.echo("=" * 50)
    
    # Afficher les compteurs
    for status, count in status_counts.items():
        status_icon = {
            'running': '🟢',
            'active': '🟢',
            'created': '⚪',
            'paused': '🟡',
            'stopped': '🔴',
            'error': '❌'
        }.get(status, '❓')
        
        click.echo(f"{status_icon} {status.title()}: {count}")
    
    # Détail des sessions actives
    if running_sessions:
        click.echo(f"\n🚀 RUNNING SESSIONS ({len(running_sessions)}):")
        for session in running_sessions:
            strategies = session.get('strategies', [])
            perf = session.get('performance', {})
            pnl = perf.get('total_pnl', 0)
            pnl_icon = "🟢" if pnl >= 0 else "🔴"
            
            click.echo(f"  📊 {session['name']} ({session['mode']})")
            click.echo(f"     Strategies: {len(strategies)}, PnL: {pnl_icon} {pnl:.2f}")
    
    click.echo(f"\n💡 Total Sessions: {len(trading_cli.sessions_registry)}")


if __name__ == '__main__':
    cli()