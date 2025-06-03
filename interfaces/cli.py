import click
import asyncio
from datetime import datetime
from typing import Optional
import pandas as pd

from data.models import SessionMode, StrategyConfig, RiskConfig
from data.persistence import DatabaseManager
from core.session_manager import SessionManager
from core.strategy_engine import StrategyEngine
from strategies.performance_tracker import PerformanceTracker


class TradingCLI:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.session_manager = SessionManager(self.db_manager)
        self.strategy_engine = StrategyEngine(self.db_manager)
        
    async def initialize(self):
        await self.session_manager.initialize()


@click.group()
@click.pass_context
def cli(ctx):
    """Trading Bot CLI - Manage trading sessions and strategies"""
    ctx.ensure_object(dict)
    cli_instance = TradingCLI()
    ctx.obj['cli'] = cli_instance


@cli.command()
@click.option('--name', required=True, help='Session name')
@click.option('--mode', type=click.Choice(['sandbox', 'paper', 'live']), default='paper', help='Trading mode')
@click.option('--strategy', default='SimpleMovingAverage', help='Strategy name')
@click.option('--pairs', default='BTCUSD', help='Trading pairs (comma-separated)')
@click.option('--timeframe', default='1h', help='Timeframe for strategy')
@click.option('--max-position', type=float, default=0.1, help='Maximum position size')
@click.option('--stop-loss', type=float, default=2.0, help='Stop loss percentage')
@click.option('--take-profit', type=float, default=4.0, help='Take profit percentage')
@click.pass_context
def create_session(ctx, name, mode, strategy, pairs, timeframe, max_position, stop_loss, take_profit):
    """Create a new trading session"""
    async def _create():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        # Parse pairs
        pair_list = [p.strip() for p in pairs.split(',')]
        
        # Create strategy config
        strategy_config = StrategyConfig(
            name=strategy,
            parameters={
                'short_window': 10,
                'long_window': 30,
                'max_position_size': max_position
            },
            timeframe=timeframe,
            pairs=pair_list
        )
        
        # Create risk config
        risk_config = RiskConfig(
            max_position_size=max_position,
            stop_loss_pct=stop_loss,
            take_profit_pct=take_profit,
            max_daily_loss=10.0,
            max_exposure_pct=50.0
        )
        
        session = await cli_instance.session_manager.create_session(
            name=name,
            mode=SessionMode(mode),
            strategy=strategy_config
        )
        
        click.echo(f"Created session: {session.id}")
        click.echo(f"Name: {session.name}")
        click.echo(f"Mode: {session.mode.value}")
        click.echo(f"Strategy: {session.strategy.name}")
        click.echo(f"Pairs: {', '.join(session.strategy.pairs)}")
    
    asyncio.run(_create())


@cli.command()
@click.pass_context
def list_sessions(ctx):
    """List all trading sessions"""
    async def _list():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        sessions = await cli_instance.session_manager.list_sessions()
        
        if not sessions:
            click.echo("No sessions found.")
            return
        
        click.echo(f"{'ID':<36} {'Name':<20} {'Mode':<10} {'Status':<10} {'Strategy':<20} {'Created'}")
        click.echo("-" * 120)
        
        for session in sessions:
            created = session.created_at.strftime("%Y-%m-%d %H:%M")
            click.echo(f"{session.id:<36} {session.name:<20} {session.mode.value:<10} {session.status.value:<10} {session.strategy.name:<20} {created}")
    
    asyncio.run(_list())


@cli.command()
@click.argument('session_id')
@click.option('--api-key', help='Kraken API key')
@click.option('--api-secret', help='Kraken API secret')
@click.pass_context
def start_session(ctx, session_id, api_key, api_secret):
    """Start a trading session"""
    async def _start():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        # Start the session
        success = await cli_instance.session_manager.start_session(session_id)
        if not success:
            click.echo(f"Failed to start session {session_id}")
            return
        
        # Get session details
        session = await cli_instance.session_manager.get_session(session_id)
        if not session:
            click.echo(f"Session {session_id} not found")
            return
        
        # Start the strategy
        strategy_success = await cli_instance.strategy_engine.start_strategy(
            session, 
            api_key or "", 
            api_secret or ""
        )
        
        if strategy_success:
            click.echo(f"Started session {session_id} ({session.name})")
        else:
            click.echo(f"Failed to start strategy for session {session_id}")
    
    asyncio.run(_start())


@cli.command()
@click.argument('session_id')
@click.pass_context
def stop_session(ctx, session_id):
    """Stop a trading session"""
    async def _stop():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        # Stop the strategy
        await cli_instance.strategy_engine.stop_strategy(session_id)
        
        # Stop the session
        success = await cli_instance.session_manager.stop_session(session_id)
        if success:
            click.echo(f"Stopped session {session_id}")
        else:
            click.echo(f"Failed to stop session {session_id}")
    
    asyncio.run(_stop())


@cli.command()
@click.argument('session_id')
@click.pass_context
def pause_session(ctx, session_id):
    """Pause a trading session"""
    async def _pause():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        # Pause the strategy
        await cli_instance.strategy_engine.pause_strategy(session_id)
        
        # Pause the session
        success = await cli_instance.session_manager.pause_session(session_id)
        if success:
            click.echo(f"Paused session {session_id}")
        else:
            click.echo(f"Failed to pause session {session_id}")
    
    asyncio.run(_pause())


@cli.command()
@click.argument('session_id')
@click.option('--detailed', is_flag=True, help='Show detailed performance metrics')
@click.option('--trades', is_flag=True, help='Show recent trades')
@click.option('--export', type=click.Path(), help='Export performance report to file')
@click.pass_context
def show_session(ctx, session_id, detailed, trades, export):
    """Show detailed information about a session with comprehensive performance metrics"""
    async def _show():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        session = await cli_instance.session_manager.get_session(session_id)
        if not session:
            click.echo(f"❌ Session {session_id} not found")
            return
        
        # Header avec émojis
        click.echo("=" * 80)
        click.echo(f"📊 SESSION DETAILS: {session.name}")
        click.echo("=" * 80)
        
        # Informations de base
        click.echo(f"🆔 ID: {session.id}")
        click.echo(f"📝 Name: {session.name}")
        click.echo(f"🎯 Mode: {session.mode.value}")
        
        # Status avec couleurs
        status_emoji = {
            'active': '🟢',
            'running': '🟢', 
            'paused': '🟡',
            'stopped': '🔴',
            'created': '⚪',
            'error': '❌'
        }
        status_icon = status_emoji.get(session.status.value.lower(), '❓')
        click.echo(f"{status_icon} Status: {session.status.value}")
        
        click.echo(f"📅 Created: {session.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo(f"🔄 Updated: {session.updated_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Stratégie
        click.echo(f"\n🎯 STRATEGY CONFIGURATION:")
        click.echo(f"  📈 Name: {session.strategy.name}")
        click.echo(f"  💰 Pairs: {', '.join(session.strategy.pairs)}")
        click.echo(f"  ⏰ Timeframe: {session.strategy.timeframe}")
        click.echo(f"  ⚙️ Parameters: {session.strategy.parameters}")
        
        # Risk Management
        click.echo(f"\n🛡️ RISK MANAGEMENT:")
        click.echo(f"  📏 Max Position Size: {session.risk_params.max_position_size}")
        click.echo(f"  🛑 Stop Loss: {session.risk_params.stop_loss_pct}%")
        click.echo(f"  🎯 Take Profit: {session.risk_params.take_profit_pct}%")
        click.echo(f"  💸 Max Daily Loss: {session.risk_params.max_daily_loss}")
        click.echo(f"  📊 Max Exposure: {session.risk_params.max_exposure_pct}%")
        
        # Performance de base (existant)
        click.echo(f"\n💰 BASIC PERFORMANCE:")
        pnl_icon = "🟢" if session.performance_metrics.total_pnl >= 0 else "🔴"
        click.echo(f"  {pnl_icon} Total PnL: {session.performance_metrics.total_pnl:.2f}")
        click.echo(f"  📊 Total Trades: {session.performance_metrics.total_trades}")
        
        win_rate_value = session.performance_metrics.win_rate
        win_rate_icon = "🎯" if win_rate_value >= 0.6 else "📈" if win_rate_value >= 0.4 else "📉"
        click.echo(f"  {win_rate_icon} Win Rate: {win_rate_value:.2%}")
        
        # Tentative d'obtenir des métriques avancées depuis la stratégie
        try:
            # Essayer d'accéder aux métriques avancées si disponibles
            strategy_engine = cli_instance.strategy_engine
            
            # Vérifier si la stratégie a un performance tracker
            advanced_metrics = None
            if hasattr(strategy_engine, 'get_strategy_performance'):
                advanced_metrics = await strategy_engine.get_strategy_performance(session_id)
            
            if advanced_metrics or detailed:
                click.echo(f"\n📊 ADVANCED PERFORMANCE METRICS:")
                
                if advanced_metrics:
                    # Afficher les métriques avancées si disponibles
                    metrics = advanced_metrics
                    
                    # Métriques financières avancées
                    click.echo(f"  💵 Current Balance: {metrics.get('current_capital', 'N/A')}")
                    click.echo(f"  📈 ROI: {metrics.get('total_pnl_pct', 0):.2f}%")
                    click.echo(f"  📉 Max Drawdown: {metrics.get('max_drawdown_pct', 0):.2f}%")
                    click.echo(f"  🎯 Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.3f}")
                    click.echo(f"  📊 Profit Factor: {metrics.get('profit_factor', 0):.2f}")
                    
                    # Statistiques des trades
                    click.echo(f"  🏆 Winning Trades: {metrics.get('winning_trades', 0)}")
                    click.echo(f"  💔 Losing Trades: {metrics.get('losing_trades', 0)}")
                    click.echo(f"  📊 Avg Win: {metrics.get('avg_win', 0):.2f}")
                    click.echo(f"  📉 Avg Loss: {metrics.get('avg_loss', 0):.2f}")
                    
                    # Métriques de consistance
                    click.echo(f"  🔥 Max Consecutive Wins: {metrics.get('max_consecutive_wins', 0)}")
                    click.echo(f"  ❄️ Max Consecutive Losses: {metrics.get('max_consecutive_losses', 0)}")
                    
                else:
                    click.echo("  ⚠️ Advanced metrics not available for this session")
                    click.echo("  💡 Start the session to begin tracking detailed performance")
                
        except Exception as e:
            if detailed:
                click.echo(f"\n⚠️ Could not retrieve advanced metrics: {e}")
        
        # Status de la stratégie en cours
        try:
            status = await cli_instance.strategy_engine.get_strategy_status(session_id)
            if status:
                click.echo(f"\n🎮 STRATEGY STATUS:")
                running_icon = "🟢" if status.get('is_running', False) else "🔴"
                click.echo(f"  {running_icon} Running: {status.get('is_running', False)}")
                click.echo(f"  🕐 Last Update: {status.get('last_update', 'N/A')}")
                
                positions = status.get('positions', {})
                if positions:
                    click.echo(f"  📈 Open Positions: {len(positions)}")
                    for pair, position in positions.items():
                        click.echo(f"    - {pair}: {position}")
                else:
                    click.echo(f"  📈 Open Positions: None")
                    
        except Exception as e:
            click.echo(f"\n⚠️ Strategy status unavailable: {e}")
        
        # Affichage des trades récents si demandé
        if trades:
            click.echo(f"\n📋 RECENT TRADES:")
            try:
                # Essayer d'obtenir l'historique des trades
                recent_trades = await cli_instance.db_manager.get_trades_by_session(session_id, limit=10)
                
                if recent_trades:
                    click.echo(f"{'Date':<20} {'Side':<6} {'Symbol':<10} {'Amount':<12} {'Price':<12} {'PnL':<12}")
                    click.echo("-" * 80)
                    
                    for trade in recent_trades:
                        pnl_str = f"{trade.amount * (trade.price - trade.entry_price) if hasattr(trade, 'entry_price') else 'N/A'}"
                        click.echo(f"{trade.timestamp.strftime('%Y-%m-%d %H:%M'):<20} "
                                  f"{trade.side:<6} {trade.symbol:<10} {trade.amount:<12.4f} "
                                  f"{trade.price:<12.2f} {pnl_str:<12}")
                else:
                    click.echo("  No trades found for this session")
                    
            except Exception as e:
                click.echo(f"  ⚠️ Could not retrieve trades: {e}")
        
        # Export si demandé
        if export:
            try:
                import json
                
                # Préparer les données d'export
                export_data = {
                    'session_id': session.id,
                    'session_name': session.name,
                    'mode': session.mode.value,
                    'status': session.status.value,
                    'created_at': session.created_at.isoformat(),
                    'updated_at': session.updated_at.isoformat(),
                    'strategy': {
                        'name': session.strategy.name,
                        'pairs': session.strategy.pairs,
                        'timeframe': session.strategy.timeframe,
                        'parameters': session.strategy.parameters
                    },
                    'risk_params': {
                        'max_position_size': session.risk_params.max_position_size,
                        'stop_loss_pct': session.risk_params.stop_loss_pct,
                        'take_profit_pct': session.risk_params.take_profit_pct,
                        'max_daily_loss': session.risk_params.max_daily_loss,
                        'max_exposure_pct': session.risk_params.max_exposure_pct
                    },
                    'performance': {
                        'total_pnl': session.performance_metrics.total_pnl,
                        'total_trades': session.performance_metrics.total_trades,
                        'win_rate': session.performance_metrics.win_rate
                    },
                    'exported_at': datetime.now().isoformat()
                }
                
                with open(export, 'w') as f:
                    json.dump(export_data, f, indent=2)
                
                click.echo(f"\n💾 Performance report exported to: {export}")
                
            except Exception as e:
                click.echo(f"\n❌ Export failed: {e}")
        
        click.echo("\n" + "=" * 80)
        click.echo("💡 Use --detailed for advanced metrics, --trades for recent trades, --export for JSON report")
    
    asyncio.run(_show())


@cli.command()
@click.argument('session_id')
@click.argument('new_name')
@click.pass_context
def duplicate_session(ctx, session_id, new_name):
    """Duplicate an existing session with a new name"""
    async def _duplicate():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        new_session = await cli_instance.session_manager.duplicate_session(session_id, new_name)
        if new_session:
            click.echo(f"Duplicated session {session_id} as {new_session.id} ({new_name})")
        else:
            click.echo(f"Failed to duplicate session {session_id}")
    
    asyncio.run(_duplicate())


@cli.command()
@click.argument('session_id')
@click.pass_context
def delete_session(ctx, session_id):
    """Delete a trading session"""
    async def _delete():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        success = await cli_instance.session_manager.delete_session(session_id)
        if success:
            click.echo(f"Deleted session {session_id}")
        else:
            click.echo(f"Failed to delete session {session_id}")
    
    asyncio.run(_delete())


@cli.command()
@click.pass_context
def status(ctx):
    """Show status of all active sessions"""
    async def _status():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        active_sessions = cli_instance.session_manager.get_active_sessions()
        active_strategies = cli_instance.strategy_engine.get_active_strategies()
        
        click.echo(f"Active Sessions: {len(active_sessions)}")
        click.echo(f"Running Strategies: {len(active_strategies)}")
        
        if active_sessions:
            click.echo("\nActive Sessions:")
            for session in active_sessions:
                strategy_running = session.id in active_strategies
                click.echo(f"  {session.id} ({session.name}) - Strategy: {'Running' if strategy_running else 'Stopped'}")
    
    asyncio.run(_status())


@cli.command()
@click.option('--limit', default=10, type=int, help='Number of sessions to show')
@click.option('--sort-by', type=click.Choice(['pnl', 'trades', 'win_rate', 'created']), 
              default='pnl', help='Sort criterion')
@click.pass_context
def performance(ctx, limit, sort_by):
    """Show performance summary of all sessions"""
    async def _performance():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        sessions = await cli_instance.session_manager.list_sessions()
        
        if not sessions:
            click.echo("No sessions found.")
            return
        
        # Préparer les données de performance
        performance_data = []
        for session in sessions:
            performance_data.append({
                'id': session.id[:8] + "...",  # Truncate ID for display
                'name': session.name[:20],     # Truncate name
                'mode': session.mode.value,
                'status': session.status.value,
                'pnl': session.performance_metrics.total_pnl,
                'trades': session.performance_metrics.total_trades,
                'win_rate': session.performance_metrics.win_rate,
                'created': session.created_at,
                'strategy': session.strategy.name[:15]  # Truncate strategy name
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
        click.echo(f"{'ID':<12} {'Name':<22} {'Strategy':<17} {'Mode':<8} {'Status':<10} {'PnL':<12} {'Trades':<8} {'Win Rate':<10}")
        click.echo("-" * 100)
        
        for data in performance_data:
            # Icons pour PnL
            pnl_icon = "🟢" if data['pnl'] >= 0 else "🔴"
            pnl_str = f"{pnl_icon} {data['pnl']:.2f}"
            
            # Icons pour status
            status_icons = {'active': '🟢', 'running': '🟢', 'paused': '🟡', 'stopped': '🔴', 'created': '⚪'}
            status_icon = status_icons.get(data['status'], '❓')
            status_str = f"{status_icon} {data['status'][:8]}"
            
            # Win rate avec icon
            win_rate_icon = "🎯" if data['win_rate'] >= 0.6 else "📈" if data['win_rate'] >= 0.4 else "📉"
            win_rate_str = f"{win_rate_icon} {data['win_rate']:.1%}"
            
            click.echo(f"{data['id']:<12} {data['name']:<22} {data['strategy']:<17} "
                      f"{data['mode']:<8} {status_str:<13} {pnl_str:<15} "
                      f"{data['trades']:<8} {win_rate_str:<13}")
        
        click.echo("\n💡 Use 'show_session <session_id> --detailed' for comprehensive metrics")
    
    asyncio.run(_performance())


@cli.command()
@click.argument('session_ids', nargs=-1, required=True)
@click.pass_context
def compare(ctx, session_ids):
    """Compare performance between multiple sessions"""
    async def _compare():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        sessions_data = []
        
        for session_id in session_ids:
            session = await cli_instance.session_manager.get_session(session_id)
            if not session:
                click.echo(f"⚠️ Session '{session_id}' not found, skipping")
                continue
            
            sessions_data.append({
                'id': session.id,
                'name': session.name,
                'pnl': session.performance_metrics.total_pnl,
                'trades': session.performance_metrics.total_trades,
                'win_rate': session.performance_metrics.win_rate,
                'strategy': session.strategy.name,
                'mode': session.mode.value
            })
        
        if len(sessions_data) < 2:
            click.echo("❌ Need at least 2 valid sessions to compare")
            return
        
        click.echo(f"\n📊 COMPARISON OF {len(sessions_data)} SESSIONS:")
        click.echo("=" * 80)
        
        # Tableau de comparaison
        click.echo(f"{'Session':<25} {'Strategy':<20} {'PnL':<12} {'Trades':<8} {'Win Rate':<10}")
        click.echo("-" * 80)
        
        for data in sessions_data:
            pnl_icon = "🟢" if data['pnl'] >= 0 else "🔴"
            win_rate_icon = "🎯" if data['win_rate'] >= 0.6 else "📈" if data['win_rate'] >= 0.4 else "📉"
            
            session_display = f"{data['name'][:20]} ({data['mode']})"
            
            click.echo(f"{session_display:<25} {data['strategy'][:20]:<20} "
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
    
    asyncio.run(_compare())


if __name__ == '__main__':
    cli()