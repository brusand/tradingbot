import click
import asyncio
import json
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd

from data.models import SessionMode, StrategyConfig, RiskConfig
from data.persistence import DatabaseManager
from core.session_manager import SessionManager
from core.multi_session_manager import MultiSessionManager
from strategies.performance_tracker import PerformanceTracker
from config.settings import settings


class TradingCLIV2:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.session_manager = SessionManager(self.db_manager)
        self.multi_session_manager = MultiSessionManager(self.db_manager)
        
    async def initialize(self):
        await self.multi_session_manager.initialize()


@click.group()
@click.pass_context
def cli(ctx):
    """Trading Bot CLI v2 - Advanced Multi-Session Management"""
    ctx.ensure_object(dict)
    cli_instance = TradingCLIV2()
    ctx.obj['cli'] = cli_instance


# Enhanced session creation with bulk operations
@cli.command()
@click.option('--name', required=True, help='Session name')
@click.option('--mode', type=click.Choice(['sandbox', 'paper', 'live']), default='paper', help='Trading mode')
@click.option('--strategy', default='SimpleMovingAverage', help='Strategy name')
@click.option('--pairs', default='BTCUSD', help='Trading pairs (comma-separated)')
@click.option('--timeframe', default='1h', help='Timeframe for strategy')
@click.option('--max-position', type=float, default=0.1, help='Maximum position size')
@click.option('--stop-loss', type=float, default=2.0, help='Stop loss percentage')
@click.option('--take-profit', type=float, default=4.0, help='Take profit percentage')
@click.option('--count', type=int, default=1, help='Number of sessions to create with different names')
@click.pass_context
def create_sessions(ctx, name, mode, strategy, pairs, timeframe, max_position, stop_loss, take_profit, count):
    """Create one or multiple trading sessions"""
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
        
        created_sessions = []
        
        for i in range(count):
            session_name = f"{name}_{i+1}" if count > 1 else name
            
            session = await cli_instance.session_manager.create_session(
                name=session_name,
                mode=SessionMode(mode),
                strategy=strategy_config,
                risk_params=risk_config
            )
            
            created_sessions.append(session)
            click.echo(f"Created session {i+1}/{count}: {session.id} ({session_name})")
        
        if count > 1:
            click.echo(f"\nSuccessfully created {len(created_sessions)} sessions")
            click.echo("Session IDs:")
            for session in created_sessions:
                click.echo(f"  {session.id} - {session.name}")
    
    asyncio.run(_create())


# Multi-session start command
@cli.command()
@click.option('--sessions', help='Comma-separated session IDs')
@click.option('--pattern', help='Start sessions matching name pattern')
@click.option('--mode', type=click.Choice(['sandbox', 'paper', 'live']), help='Start sessions with specific mode')
@click.option('--all', is_flag=True, help='Start all available sessions')
@click.option('--api-key', help='Kraken API key (for live/sandbox)')
@click.option('--api-secret', help='Kraken API secret (for live/sandbox)')
@click.option('--dry-run', is_flag=True, help='Show what would be started without actually starting')
@click.pass_context
def start_sessions(ctx, sessions, pattern, mode, all, api_key, api_secret, dry_run):
    """Start multiple trading sessions in parallel"""
    async def _start():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        # Get session IDs to start
        session_ids = await _get_session_ids(cli_instance, sessions, pattern, mode, all)
        
        if not session_ids:
            click.echo("No sessions found matching criteria")
            return
        
        click.echo(f"Found {len(session_ids)} sessions to start:")
        for session_id in session_ids:
            session = await cli_instance.session_manager.get_session(session_id)
            if session:
                click.echo(f"  {session_id} - {session.name} ({session.mode.value})")
        
        if dry_run:
            click.echo("\nDry run mode - sessions not started")
            return
        
        # Check capacity
        system_stats = await cli_instance.multi_session_manager.get_system_stats()
        if len(session_ids) > system_stats['available_slots']:
            click.echo(f"Warning: Only {system_stats['available_slots']} slots available, but {len(session_ids)} sessions requested")
            if not click.confirm("Continue anyway? (excess sessions will fail)"):
                return
        
        # Prepare credentials
        api_keys = {sid: api_key for sid in session_ids} if api_key else None
        api_secrets = {sid: api_secret for sid in session_ids} if api_secret else None
        
        click.echo(f"\nStarting {len(session_ids)} sessions...")
        results = await cli_instance.multi_session_manager.start_sessions(
            session_ids, api_keys, api_secrets
        )
        
        # Display results
        success_count = sum(1 for success in results.values() if success)
        click.echo(f"\nResults: {success_count}/{len(session_ids)} sessions started successfully")
        
        for session_id, success in results.items():
            session = await cli_instance.session_manager.get_session(session_id)
            status = "✓ Started" if success else "✗ Failed"
            name = session.name if session else "Unknown"
            click.echo(f"  {status} - {session_id} ({name})")
    
    asyncio.run(_start())


# Multi-session stop command
@cli.command()
@click.option('--sessions', help='Comma-separated session IDs')
@click.option('--pattern', help='Stop sessions matching name pattern')
@click.option('--all', is_flag=True, help='Stop all running sessions')
@click.option('--force', is_flag=True, help='Force kill sessions')
@click.option('--dry-run', is_flag=True, help='Show what would be stopped without actually stopping')
@click.pass_context
def stop_sessions(ctx, sessions, pattern, all, force, dry_run):
    """Stop multiple trading sessions"""
    async def _stop():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        # Get running session IDs
        if all:
            running_processes = cli_instance.multi_session_manager.list_running_sessions()
            session_ids = [p.session_id for p in running_processes]
        elif sessions:
            session_ids = [s.strip() for s in sessions.split(',')]
            # Filter to only running sessions
            running_processes = cli_instance.multi_session_manager.list_running_sessions()
            running_ids = {p.session_id for p in running_processes}
            session_ids = [sid for sid in session_ids if sid in running_ids]
        elif pattern:
            # Find running sessions matching pattern
            running_processes = cli_instance.multi_session_manager.list_running_sessions()
            session_ids = []
            for process in running_processes:
                session = await cli_instance.session_manager.get_session(process.session_id)
                if session and pattern.lower() in session.name.lower():
                    session_ids.append(process.session_id)
        else:
            click.echo("Must specify --sessions, --pattern, or --all")
            return
        
        if not session_ids:
            click.echo("No running sessions found matching criteria")
            return
        
        click.echo(f"Found {len(session_ids)} running sessions to stop:")
        for session_id in session_ids:
            session = await cli_instance.session_manager.get_session(session_id)
            process = cli_instance.multi_session_manager.get_session_process(session_id)
            name = session.name if session else "Unknown"
            runtime = ""
            if process:
                delta = datetime.utcnow() - process.started_at
                runtime = f"(running {delta.seconds//60}m)"
            click.echo(f"  {session_id} - {name} {runtime}")
        
        if dry_run:
            click.echo("\nDry run mode - sessions not stopped")
            return
        
        action = "Force killing" if force else "Stopping"
        click.echo(f"\n{action} {len(session_ids)} sessions...")
        
        if force:
            results = await cli_instance.multi_session_manager.kill_sessions(session_ids)
        else:
            results = await cli_instance.multi_session_manager.stop_sessions(session_ids)
        
        # Display results
        success_count = sum(1 for success in results.values() if success)
        click.echo(f"\nResults: {success_count}/{len(session_ids)} sessions stopped successfully")
        
        for session_id, success in results.items():
            session = await cli_instance.session_manager.get_session(session_id)
            status = "✓ Stopped" if success else "✗ Failed"
            name = session.name if session else "Unknown"
            click.echo(f"  {status} - {session_id} ({name})")
    
    asyncio.run(_stop())


# List running sessions with process info
@cli.command()
@click.option('--format', type=click.Choice(['table', 'json']), default='table', help='Output format')
@click.option('--watch', is_flag=True, help='Watch mode - refresh every 5 seconds')
@click.pass_context
def list_running(ctx, format, watch):
    """List all running sessions with process information"""
    async def _list():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        async def format_output(processes, system_stats):
            if format == 'json':
                output = {
                    'system_stats': system_stats,
                    'running_sessions': []
                }
                for process in processes:
                    output['running_sessions'].append({
                        'session_id': process.session_id,
                        'started_at': process.started_at.isoformat(),
                        'status': process.status,
                        'cpu_percent': process.cpu_percent,
                        'memory_mb': process.memory_mb,
                        'runtime_seconds': (datetime.utcnow() - process.started_at).seconds
                    })
                return json.dumps(output, indent=2)
            else:
                # Table format
                output = []
                output.append(f"System: {system_stats['active_sessions']}/{system_stats['max_concurrent']} sessions")
                output.append(f"CPU: {system_stats['system_cpu_percent']:.1f}% | Memory: {system_stats['system_memory_percent']:.1f}%")
                output.append("")
                
                if processes:
                    output.append(f"{'Session ID':<36} {'Name':<20} {'Status':<10} {'Runtime':<10} {'CPU%':<8} {'Mem(MB)':<8}")
                    output.append("-" * 100)
                    
                    for process in processes:
                        session = await cli_instance.session_manager.get_session(process.session_id)
                        name = session.name if session else "Unknown"
                        runtime = f"{(datetime.utcnow() - process.started_at).seconds//60}m"
                        output.append(f"{process.session_id:<36} {name:<20} {process.status:<10} {runtime:<10} {process.cpu_percent:<8.1f} {process.memory_mb:<8.1f}")
                else:
                    output.append("No sessions currently running")
                
                return "\n".join(output)
        
        if watch:
            try:
                while True:
                    click.clear()
                    processes = cli_instance.multi_session_manager.list_running_sessions()
                    system_stats = await cli_instance.multi_session_manager.get_system_stats()
                    output = await format_output(processes, system_stats)
                    click.echo(output)
                    click.echo(f"\nLast updated: {datetime.now().strftime('%H:%M:%S')} (Press Ctrl+C to exit)")
                    await asyncio.sleep(5)
            except KeyboardInterrupt:
                click.echo("\nExiting watch mode")
        else:
            processes = cli_instance.multi_session_manager.list_running_sessions()
            system_stats = await cli_instance.multi_session_manager.get_system_stats()
            output = await format_output(processes, system_stats)
            click.echo(output)
    
    asyncio.run(_list())


# Kill sessions (force stop)
@cli.command()
@click.option('--sessions', help='Comma-separated session IDs to kill')
@click.option('--all', is_flag=True, help='Kill all running sessions')
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def kill_sessions(ctx, sessions, all, confirm):
    """Force kill running sessions (immediate termination)"""
    async def _kill():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        if all:
            running_processes = cli_instance.multi_session_manager.list_running_sessions()
            session_ids = [p.session_id for p in running_processes]
        elif sessions:
            session_ids = [s.strip() for s in sessions.split(',')]
        else:
            click.echo("Must specify --sessions or --all")
            return
        
        if not session_ids:
            click.echo("No sessions to kill")
            return
        
        click.echo(f"⚠️  WARNING: About to force kill {len(session_ids)} sessions")
        for session_id in session_ids:
            session = await cli_instance.session_manager.get_session(session_id)
            name = session.name if session else "Unknown"
            click.echo(f"  {session_id} - {name}")
        
        if not confirm and not click.confirm("\nThis will immediately terminate the processes. Continue?"):
            click.echo("Cancelled")
            return
        
        results = await cli_instance.multi_session_manager.kill_sessions(session_ids)
        
        success_count = sum(1 for success in results.values() if success)
        click.echo(f"\nKilled: {success_count}/{len(session_ids)} sessions")
        
        for session_id, success in results.items():
            status = "✓ Killed" if success else "✗ Failed"
            click.echo(f"  {status} - {session_id}")
    
    asyncio.run(_kill())


# Enhanced session listing with filtering
@cli.command()
@click.option('--mode', type=click.Choice(['sandbox', 'paper', 'live']), help='Filter by mode')
@click.option('--status', type=click.Choice(['created', 'running', 'paused', 'stopped', 'error']), help='Filter by status')
@click.option('--pattern', help='Filter by name pattern')
@click.option('--format', type=click.Choice(['table', 'json']), default='table', help='Output format')
@click.option('--limit', type=int, help='Limit number of results')
@click.pass_context
def list_sessions(ctx, mode, status, pattern, format, limit):
    """List trading sessions with advanced filtering"""
    async def _list():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        sessions = await cli_instance.session_manager.list_sessions()
        
        # Apply filters
        if mode:
            sessions = [s for s in sessions if s.mode.value == mode]
        if status:
            sessions = [s for s in sessions if s.status.value == status]
        if pattern:
            sessions = [s for s in sessions if pattern.lower() in s.name.lower()]
        if limit:
            sessions = sessions[:limit]
        
        if format == 'json':
            output = {
                'count': len(sessions),
                'sessions': [
                    {
                        'id': s.id,
                        'name': s.name,
                        'mode': s.mode.value,
                        'status': s.status.value,
                        'strategy': s.strategy.name,
                        'created_at': s.created_at.isoformat(),
                        'pairs': s.strategy.pairs,
                        'performance': {
                            'total_pnl': s.performance_metrics.total_pnl,
                            'total_trades': s.performance_metrics.total_trades,
                            'win_rate': s.performance_metrics.win_rate
                        }
                    }
                    for s in sessions
                ]
            }
            click.echo(json.dumps(output, indent=2))
        else:
            if not sessions:
                click.echo("No sessions found matching criteria.")
                return
            
            click.echo(f"Found {len(sessions)} sessions:")
            click.echo(f"{'ID':<36} {'Name':<20} {'Mode':<10} {'Status':<10} {'Strategy':<20} {'PnL':<10} {'Created'}")
            click.echo("-" * 130)
            
            for session in sessions:
                created = session.created_at.strftime("%m-%d %H:%M")
                pnl = f"{session.performance_metrics.total_pnl:.2f}"
                click.echo(f"{session.id:<36} {session.name:<20} {session.mode.value:<10} {session.status.value:<10} {session.strategy.name:<20} {pnl:<10} {created}")
    
    asyncio.run(_list())


async def _get_session_ids(cli_instance, sessions_param, pattern, mode, all_flag):
    """Helper to get session IDs based on various criteria"""
    if sessions_param:
        return [s.strip() for s in sessions_param.split(',')]
    
    all_sessions = await cli_instance.session_manager.list_sessions()
    
    # Apply filters
    if mode:
        all_sessions = [s for s in all_sessions if s.mode.value == mode]
    if pattern:
        all_sessions = [s for s in all_sessions if pattern.lower() in s.name.lower()]
    if all_flag:
        return [s.id for s in all_sessions]
    
    return [s.id for s in all_sessions]


# System status command
@cli.command()
@click.pass_context
def system_status(ctx):
    """Show overall system status and statistics"""
    async def _status():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        system_stats = await cli_instance.multi_session_manager.get_system_stats()
        running_processes = cli_instance.multi_session_manager.list_running_sessions()
        
        click.echo("=== Trading Bot System Status ===")
        click.echo(f"Active Sessions: {system_stats['active_sessions']}/{system_stats['max_concurrent']}")
        click.echo(f"Available Slots: {system_stats['available_slots']}")
        click.echo(f"Total CPU Usage: {system_stats['total_cpu_percent']:.1f}%")
        click.echo(f"Total Memory Usage: {system_stats['total_memory_mb']:.1f} MB")
        click.echo(f"System CPU: {system_stats['system_cpu_percent']:.1f}%")
        click.echo(f"System Memory: {system_stats['system_memory_percent']:.1f}%")
        
        if running_processes:
            click.echo("\n=== Running Sessions ===")
            for process in running_processes:
                session = await cli_instance.session_manager.get_session(process.session_id)
                name = session.name if session else "Unknown"
                runtime = (datetime.utcnow() - process.started_at).seconds // 60
                click.echo(f"  {name} - {runtime}m runtime, {process.cpu_percent:.1f}% CPU, {process.memory_mb:.1f}MB")
    
    asyncio.run(_status())


# ============================================================================
# PERFORMANCE ANALYTICS COMMANDS
# ============================================================================

@cli.group()
def analytics():
    """Advanced performance analytics and reporting"""
    pass


@analytics.command()
@click.option('--days', default=30, type=int, help='Number of days to analyze')
@click.option('--min-trades', default=5, type=int, help='Minimum trades for inclusion')
@click.option('--export', type=click.Path(), help='Export detailed report to JSON')
@click.pass_context
def portfolio_report(ctx, days, min_trades, export):
    """Generate comprehensive portfolio performance report"""
    async def _report():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        click.echo("📊 PORTFOLIO PERFORMANCE REPORT")
        click.echo("=" * 50)
        
        # Get all sessions
        sessions = await cli_instance.session_manager.list_sessions()
        
        if not sessions:
            click.echo("No sessions found")
            return
        
        # Filter sessions with enough activity
        active_sessions = []
        total_portfolio_pnl = 0
        total_trades = 0
        
        for session in sessions:
            if session.performance_metrics.total_trades >= min_trades:
                active_sessions.append(session)
                total_portfolio_pnl += session.performance_metrics.total_pnl
                total_trades += session.performance_metrics.total_trades
        
        if not active_sessions:
            click.echo(f"No sessions with at least {min_trades} trades found")
            return
        
        # Portfolio summary
        click.echo(f"📈 PORTFOLIO SUMMARY:")
        click.echo(f"  🎯 Active Sessions: {len(active_sessions)}")
        click.echo(f"  💰 Total PnL: {total_portfolio_pnl:.2f}")
        click.echo(f"  📊 Total Trades: {total_trades}")
        
        if total_trades > 0:
            avg_pnl_per_trade = total_portfolio_pnl / total_trades
            click.echo(f"  📊 Avg PnL/Trade: {avg_pnl_per_trade:.2f}")
        
        # Session breakdown
        click.echo(f"\n📋 SESSION BREAKDOWN:")
        click.echo(f"{'Name':<25} {'Mode':<8} {'PnL':<12} {'Trades':<8} {'Win Rate':<10} {'Strategy'}")
        click.echo("-" * 80)
        
        # Sort by PnL
        active_sessions.sort(key=lambda s: s.performance_metrics.total_pnl, reverse=True)
        
        for session in active_sessions:
            pnl_icon = "🟢" if session.performance_metrics.total_pnl >= 0 else "🔴"
            win_rate = session.performance_metrics.win_rate
            win_rate_str = f"{win_rate:.1%}"
            
            click.echo(f"{session.name[:24]:<25} {session.mode.value:<8} "
                      f"{pnl_icon} {session.performance_metrics.total_pnl:<9.2f} "
                      f"{session.performance_metrics.total_trades:<8} {win_rate_str:<10} "
                      f"{session.strategy.name}")
        
        # Risk analysis
        click.echo(f"\n⚠️ RISK ANALYSIS:")
        profitable_sessions = [s for s in active_sessions if s.performance_metrics.total_pnl > 0]
        losing_sessions = [s for s in active_sessions if s.performance_metrics.total_pnl < 0]
        
        click.echo(f"  📈 Profitable Sessions: {len(profitable_sessions)}/{len(active_sessions)} ({len(profitable_sessions)/len(active_sessions):.1%})")
        
        if losing_sessions:
            max_loss = min(s.performance_metrics.total_pnl for s in losing_sessions)
            click.echo(f"  📉 Largest Single Loss: {max_loss:.2f}")
        
        # Export detailed data if requested
        if export:
            report_data = {
                "generated_at": datetime.now().isoformat(),
                "portfolio_summary": {
                    "total_sessions": len(active_sessions),
                    "total_pnl": total_portfolio_pnl,
                    "total_trades": total_trades,
                    "profitable_sessions": len(profitable_sessions),
                    "losing_sessions": len(losing_sessions)
                },
                "sessions": []
            }
            
            for session in active_sessions:
                session_data = {
                    "id": session.id,
                    "name": session.name,
                    "mode": session.mode.value,
                    "strategy": session.strategy.name,
                    "performance": {
                        "total_pnl": session.performance_metrics.total_pnl,
                        "total_trades": session.performance_metrics.total_trades,
                        "win_rate": session.performance_metrics.win_rate
                    },
                    "risk_params": {
                        "max_position_size": session.risk_params.max_position_size,
                        "stop_loss_pct": session.risk_params.stop_loss_pct,
                        "take_profit_pct": session.risk_params.take_profit_pct
                    }
                }
                report_data["sessions"].append(session_data)
            
            with open(export, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            click.echo(f"\n💾 Detailed report exported to: {export}")
    
    asyncio.run(_report())


@analytics.command()
@click.argument('session_ids', nargs=-1, required=True)
@click.option('--metric', type=click.Choice(['pnl', 'win_rate', 'trades', 'sharpe']), 
              default='pnl', help='Primary comparison metric')
@click.pass_context
def compare_advanced(ctx, session_ids, metric):
    """Advanced comparison with statistical analysis"""
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
                'mode': session.mode.value,
                'risk_params': session.risk_params
            })
        
        if len(sessions_data) < 2:
            click.echo("❌ Need at least 2 valid sessions for advanced comparison")
            return
        
        click.echo(f"\n📊 ADVANCED COMPARISON OF {len(sessions_data)} SESSIONS")
        click.echo("=" * 60)
        
        # Statistical summary
        pnls = [s['pnl'] for s in sessions_data]
        win_rates = [s['win_rate'] for s in sessions_data]
        
        click.echo(f"\n📈 STATISTICAL SUMMARY:")
        click.echo(f"  💰 PnL Range: {min(pnls):.2f} to {max(pnls):.2f}")
        click.echo(f"  📊 Avg PnL: {sum(pnls)/len(pnls):.2f}")
        click.echo(f"  🎯 Win Rate Range: {min(win_rates):.1%} to {max(win_rates):.1%}")
        click.echo(f"  🎯 Avg Win Rate: {sum(win_rates)/len(win_rates):.1%}")
        
        # Detailed comparison table
        click.echo(f"\n📋 DETAILED COMPARISON:")
        click.echo(f"{'Session':<20} {'Strategy':<20} {'PnL':<12} {'Trades':<8} {'Win Rate':<10} {'Risk Level'}")
        click.echo("-" * 85)
        
        # Sort by chosen metric
        if metric == 'pnl':
            sessions_data.sort(key=lambda x: x['pnl'], reverse=True)
        elif metric == 'win_rate':
            sessions_data.sort(key=lambda x: x['win_rate'], reverse=True)
        elif metric == 'trades':
            sessions_data.sort(key=lambda x: x['trades'], reverse=True)
        
        for data in sessions_data:
            pnl_icon = "🟢" if data['pnl'] >= 0 else "🔴"
            win_rate_icon = "🎯" if data['win_rate'] >= 0.6 else "📈" if data['win_rate'] >= 0.4 else "📉"
            
            # Calculate risk level based on stop loss
            risk_level = "High" if data['risk_params'].stop_loss_pct < 1.5 else "Med" if data['risk_params'].stop_loss_pct < 3.0 else "Low"
            
            click.echo(f"{data['name'][:19]:<20} {data['strategy'][:19]:<20} "
                      f"{pnl_icon} {data['pnl']:<9.2f} {data['trades']:<8} "
                      f"{win_rate_icon} {data['win_rate']:<7.1%} {risk_level}")
        
        # Performance analysis
        click.echo(f"\n🔍 PERFORMANCE ANALYSIS:")
        
        best_performer = max(sessions_data, key=lambda x: x['pnl'])
        worst_performer = min(sessions_data, key=lambda x: x['pnl'])
        
        click.echo(f"  🏆 Best Performer: {best_performer['name']} (+{best_performer['pnl']:.2f})")
        click.echo(f"  📉 Worst Performer: {worst_performer['name']} ({worst_performer['pnl']:.2f})")
        
        spread = best_performer['pnl'] - worst_performer['pnl']
        click.echo(f"  📊 Performance Spread: {spread:.2f}")
        
        # Risk-Reward Analysis
        click.echo(f"\n⚖️ RISK-REWARD ANALYSIS:")
        for data in sessions_data:
            risk_reward = data['risk_params'].take_profit_pct / data['risk_params'].stop_loss_pct
            click.echo(f"  {data['name'][:15]:<16}: RR Ratio {risk_reward:.1f}:1, "
                      f"Max Pos: {data['risk_params'].max_position_size:.2f}")
    
    asyncio.run(_compare())


@analytics.command()
@click.option('--timeframe', type=click.Choice(['daily', 'weekly', 'monthly']), 
              default='daily', help='Analysis timeframe')
@click.option('--strategies', multiple=True, help='Filter by specific strategies')
@click.pass_context
def trend_analysis(ctx, timeframe, strategies):
    """Analyze performance trends over time"""
    async def _trends():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        click.echo(f"📈 PERFORMANCE TREND ANALYSIS ({timeframe.upper()})")
        click.echo("=" * 50)
        
        sessions = await cli_instance.session_manager.list_sessions()
        
        if strategies:
            sessions = [s for s in sessions if s.strategy.name in strategies]
            click.echo(f"🎯 Filtered to strategies: {', '.join(strategies)}")
        
        if not sessions:
            click.echo("No sessions found matching criteria")
            return
        
        # Group sessions by strategy
        strategy_performance = {}
        for session in sessions:
            strategy_name = session.strategy.name
            if strategy_name not in strategy_performance:
                strategy_performance[strategy_name] = []
            strategy_performance[strategy_name].append(session)
        
        click.echo(f"\n📊 STRATEGY PERFORMANCE OVERVIEW:")
        
        for strategy_name, strategy_sessions in strategy_performance.items():
            total_pnl = sum(s.performance_metrics.total_pnl for s in strategy_sessions)
            total_trades = sum(s.performance_metrics.total_trades for s in strategy_sessions)
            avg_win_rate = sum(s.performance_metrics.win_rate for s in strategy_sessions) / len(strategy_sessions)
            
            profitability = len([s for s in strategy_sessions if s.performance_metrics.total_pnl > 0]) / len(strategy_sessions)
            
            click.echo(f"\n🎯 {strategy_name}:")
            click.echo(f"  📈 Sessions: {len(strategy_sessions)}")
            click.echo(f"  💰 Total PnL: {total_pnl:.2f}")
            click.echo(f"  📊 Total Trades: {total_trades}")
            click.echo(f"  🎯 Avg Win Rate: {avg_win_rate:.1%}")
            click.echo(f"  📈 Profitability: {profitability:.1%} of sessions")
        
        # Find trends
        click.echo(f"\n🔍 TREND INSIGHTS:")
        
        # Best performing strategy
        best_strategy = max(strategy_performance.items(), 
                           key=lambda x: sum(s.performance_metrics.total_pnl for s in x[1]))
        click.echo(f"  🏆 Top Strategy: {best_strategy[0]} "
                  f"(Total PnL: {sum(s.performance_metrics.total_pnl for s in best_strategy[1]):.2f})")
        
        # Most consistent strategy (highest avg win rate)
        most_consistent = max(strategy_performance.items(),
                            key=lambda x: sum(s.performance_metrics.win_rate for s in x[1]) / len(x[1]))
        avg_wr = sum(s.performance_metrics.win_rate for s in most_consistent[1]) / len(most_consistent[1])
        click.echo(f"  🎯 Most Consistent: {most_consistent[0]} (Avg Win Rate: {avg_wr:.1%})")
        
        # Most active strategy
        most_active = max(strategy_performance.items(),
                         key=lambda x: sum(s.performance_metrics.total_trades for s in x[1]))
        total_trades = sum(s.performance_metrics.total_trades for s in most_active[1])
        click.echo(f"  📊 Most Active: {most_active[0]} ({total_trades} total trades)")
    
    asyncio.run(_trends())


@analytics.command()
@click.option('--min-correlation', type=float, default=0.7, help='Minimum correlation threshold')
@click.pass_context
def correlation_matrix(ctx, min_correlation):
    """Analyze correlation between session performances"""
    async def _correlation():
        cli_instance = ctx.obj['cli']
        await cli_instance.initialize()
        
        click.echo("🔗 SESSION CORRELATION ANALYSIS")
        click.echo("=" * 40)
        
        sessions = await cli_instance.session_manager.list_sessions()
        
        # Filter sessions with enough data
        valid_sessions = [s for s in sessions if s.performance_metrics.total_trades >= 10]
        
        if len(valid_sessions) < 2:
            click.echo("Need at least 2 sessions with 10+ trades for correlation analysis")
            return
        
        click.echo(f"📊 Analyzing {len(valid_sessions)} sessions with sufficient data")
        
        # For demonstration, calculate simple correlations based on win rates and strategies
        # In a real implementation, you'd use time-series data
        
        correlations = []
        
        for i, session1 in enumerate(valid_sessions):
            for j, session2 in enumerate(valid_sessions[i+1:], i+1):
                # Simple correlation based on strategy similarity and performance
                strategy_match = 1.0 if session1.strategy.name == session2.strategy.name else 0.3
                
                # Performance similarity (normalized)
                pnl1 = session1.performance_metrics.total_pnl
                pnl2 = session2.performance_metrics.total_pnl
                pnl_correlation = 1.0 - abs(pnl1 - pnl2) / (abs(pnl1) + abs(pnl2) + 1)
                
                # Win rate similarity
                wr1 = session1.performance_metrics.win_rate
                wr2 = session2.performance_metrics.win_rate
                wr_correlation = 1.0 - abs(wr1 - wr2)
                
                # Combined correlation
                correlation = (strategy_match * 0.5 + pnl_correlation * 0.3 + wr_correlation * 0.2)
                
                if correlation >= min_correlation:
                    correlations.append({
                        'session1': session1.name,
                        'session2': session2.name,
                        'correlation': correlation,
                        'strategy1': session1.strategy.name,
                        'strategy2': session2.strategy.name
                    })
        
        if correlations:
            click.echo(f"\n🔗 HIGH CORRELATIONS (≥{min_correlation:.1f}):")
            click.echo(f"{'Session 1':<20} {'Session 2':<20} {'Correlation':<12} {'Same Strategy'}")
            click.echo("-" * 70)
            
            correlations.sort(key=lambda x: x['correlation'], reverse=True)
            
            for corr in correlations:
                same_strategy = "✓" if corr['strategy1'] == corr['strategy2'] else "✗"
                click.echo(f"{corr['session1'][:19]:<20} {corr['session2'][:19]:<20} "
                          f"{corr['correlation']:.3f}        {same_strategy}")
            
            click.echo(f"\n💡 INSIGHTS:")
            same_strategy_corr = [c for c in correlations if c['strategy1'] == c['strategy2']]
            if same_strategy_corr:
                avg_same = sum(c['correlation'] for c in same_strategy_corr) / len(same_strategy_corr)
                click.echo(f"  🎯 Avg correlation within same strategy: {avg_same:.3f}")
            
            diff_strategy_corr = [c for c in correlations if c['strategy1'] != c['strategy2']]
            if diff_strategy_corr:
                avg_diff = sum(c['correlation'] for c in diff_strategy_corr) / len(diff_strategy_corr)
                click.echo(f"  🔄 Avg correlation across strategies: {avg_diff:.3f}")
        else:
            click.echo(f"\n📊 No high correlations found (threshold: {min_correlation:.1f})")
            click.echo("💡 This suggests good diversification across sessions")
    
    asyncio.run(_correlation())


if __name__ == '__main__':
    cli()