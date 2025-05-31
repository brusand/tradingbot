"""
TradingCLI V4 - CLI Unifié et Complet
Fusion de toutes les versions : CLI de base, CLI V2, CLI Adapted et CLI V3
Combine : workflow avancé, multi-session, analytics, configuration persistante
"""

import click
import asyncio
import json
import yaml
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
import os
import sys
import time

# Tabulate avec fallback
try:
    from tabulate import tabulate
except ImportError:
    def tabulate(data, headers=None, tablefmt='grid'):
        """Simple fallback for tabulate"""
        if not data:
            return "No data"
        
        result = []
        if headers:
            result.append(" | ".join(str(h) for h in headers))
            result.append("-" * (len(" | ".join(str(h) for h in headers))))
        
        for row in data:
            result.append(" | ".join(str(cell) for cell in row))
        
        return "\n".join(result)

# Imports du système
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports core système de base
from data.models import StrategyConfig, SessionMode, RiskConfig
from data.persistence import DatabaseManager
from core.session_manager import SessionManager
from core.strategy_engine import StrategyEngine

# Imports workflow avancé
try:
    from core.strategy_workflow import TradingStrategy, IndicatorConfig, SignalRule
    from core.indicators_service import IndicatorsService
    from core.strategy_manager_enhanced import StrategyManagerEnhanced, EnhancedStrategyConfig
    from core.pubsub_engine import PubSubEngine
    from core.channels import CHANNELS
    WORKFLOW_AVAILABLE = True
except ImportError:
    WORKFLOW_AVAILABLE = False
    click.echo("⚠️ Workflow avancé non disponible")

# Imports multi-session (optionnel)
try:
    from core.multi_session_manager import MultiSessionManager
    MULTI_SESSION_AVAILABLE = True
except ImportError:
    MULTI_SESSION_AVAILABLE = False

class TradingCLI:
    """CLI Unifié avec support de toutes les fonctionnalités"""
    
    def __init__(self, config_path: str = "config/trading_config.yaml"):
        self.config_path = config_path
        
        # Managers de base
        self.db_manager = None
        self.session_manager = None
        self.strategy_engine = None
        
        # Workflow avancé (optionnel)
        self.pubsub = None
        self.strategy_manager_enhanced = None
        self.workflow_running = False
        
        # Multi-session (optionnel)
        self.multi_session_manager = None
        
        # Registres de configuration (CLI Adapted)
        self.strategies_registry = {}
        self.indicators_registry = {}
        self.risk_profiles_registry = {}
        self.sessions_registry = {}
        
        # Cache pour performances
        self._session_cache = {}
        self._last_cache_update = 0
        
        # Charger configuration
        self._load_configuration()
        self._initialize_defaults()
    
    def _load_configuration(self):
        """Charge la configuration depuis YAML (CLI Adapted)"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config = yaml.safe_load(f) or {}
                    self.strategies_registry = config.get('strategies', {})
                    self.indicators_registry = config.get('indicators', {})
                    self.risk_profiles_registry = config.get('risk_profiles', {})
                    self.sessions_registry = config.get('sessions', {})
            except Exception as e:
                click.echo(f"⚠️ Erreur de chargement config: {e}")
    
    def _save_configuration(self):
        """Sauvegarde la configuration (CLI Adapted)"""
        config = {
            'strategies': self.strategies_registry,
            'indicators': self.indicators_registry,
            'risk_profiles': self.risk_profiles_registry,
            'sessions': self.sessions_registry
        }
        
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        try:
            with open(self.config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, indent=2)
        except Exception as e:
            click.echo(f"⚠️ Erreur de sauvegarde config: {e}")
    
    def _initialize_defaults(self):
        """Initialise les profils par défaut"""
        if 'default' not in self.risk_profiles_registry:
            self.risk_profiles_registry['default'] = {
                'name': 'Profil par défaut',
                'position_size_method': 'percent',
                'position_size_value': 2.0,
                'max_concurrent_trades': 3,
                'max_daily_loss': 500.0,
                'stop_loss_percent': 2.0,
                'take_profit_percent': 4.0,
                'trailing_stop': False
            }
    
    async def initialize_system(self):
        """Initialise le système complet"""
        try:
            # Base system
            self.db_manager = DatabaseManager()
            self.session_manager = SessionManager(self.db_manager)
            self.strategy_engine = StrategyEngine(self.db_manager)
            
            # Multi-session si disponible
            if MULTI_SESSION_AVAILABLE:
                self.multi_session_manager = MultiSessionManager(self.db_manager)
            
            # Workflow avancé si disponible
            if WORKFLOW_AVAILABLE:
                self.pubsub = PubSubEngine()
                await self.pubsub.start()
                self.strategy_manager_enhanced = StrategyManagerEnhanced(self.db_manager, self.pubsub)
                await self.strategy_manager_enhanced.initialize()
                self.workflow_running = True
            
            return True
        except Exception as e:
            click.echo(f"❌ Erreur d'initialisation: {e}")
            return False
    
    async def shutdown_system(self):
        """Arrêt propre du système"""
        try:
            if self.workflow_running and self.strategy_manager_enhanced:
                await self.strategy_manager_enhanced.shutdown()
            
            if self.pubsub:
                await self.pubsub.stop()
            
            self.workflow_running = False
            
        except Exception as e:
            click.echo(f"⚠️ Erreur lors de l'arrêt: {e}")
    
    def convert_to_enhanced_config(self, strategy_data: dict) -> 'EnhancedStrategyConfig':
        """Convertit stratégie du registre vers EnhancedStrategyConfig"""
        if not WORKFLOW_AVAILABLE:
            return None
        
        # Créer RiskConfig depuis strategy risk_management
        risk_config = None
        if 'risk_management' in strategy_data:
            risk_mgmt = strategy_data['risk_management']
            risk_config = RiskConfig(
                max_position_size=risk_mgmt.get('max_position_size', 0.1),
                stop_loss_pct=risk_mgmt.get('stop_loss_percent', 2.0),
                take_profit_pct=risk_mgmt.get('take_profit_percent', 4.0),
                max_daily_loss=risk_mgmt.get('max_daily_loss', 500.0),
                max_exposure_pct=risk_mgmt.get('max_exposure_pct', 10.0),
                risk_ratio=risk_mgmt.get('risk_ratio', 2.0)
            )
        
        base_config = StrategyConfig(
            name=strategy_data['name'],
            parameters=strategy_data.get('parameters', {}),
            timeframe=strategy_data.get('timeframe', '5m'),
            pairs=[strategy_data.get('symbol', 'BTCUSD')],
            risk_config=risk_config
        )
        
        enhanced = EnhancedStrategyConfig(base_config)
        
        # Appliquer indicateurs personnalisés
        if 'indicators' in strategy_data:
            enhanced.indicators = {}
            for ind_name, ind_config in strategy_data['indicators'].items():
                enhanced.indicators[ind_name] = IndicatorConfig(
                    type=ind_config['type'],
                    parameters=ind_config['parameters'],
                    source=ind_config.get('source', 'close')
                )
        
        # Appliquer règles de signaux
        if 'signal_rules' in strategy_data:
            enhanced.signal_rules = []
            if strategy_data['signal_rules'].get('long_condition'):
                enhanced.signal_rules.append(SignalRule(
                    name="custom_long",
                    signal_type="LONG",
                    condition=strategy_data['signal_rules']['long_condition'],
                    priority=2
                ))
            if strategy_data['signal_rules'].get('short_condition'):
                enhanced.signal_rules.append(SignalRule(
                    name="custom_short",
                    signal_type="SHORT",
                    condition=strategy_data['signal_rules']['short_condition'],
                    priority=2
                ))
        
        return enhanced

# Commande principale
@click.group()
@click.pass_context
def cli(ctx):
    """🚀 TradingCLI V4 - Interface Unifiée Complète"""
    ctx.ensure_object(dict)
    ctx.obj['cli'] = TradingCLI()

# ============================================================================
# COMMANDES SYSTÈME (V3 + améliorations)
# ============================================================================

@cli.group()
def system():
    """🔧 Gestion du système de trading"""
    pass

@system.command('start')
@click.option('--mode', type=click.Choice(['basic', 'workflow', 'multi']), 
              default='basic', help='Mode de démarrage')
@click.pass_context
def start_system(ctx, mode):
    """Démarrer le système"""
    trading_cli = ctx.obj['cli']
    
    async def _start():
        click.echo(f"🚀 Démarrage du système en mode {mode}...")
        
        success = await trading_cli.initialize_system()
        if success:
            if mode == 'workflow' and WORKFLOW_AVAILABLE:
                click.echo("✅ Système workflow avancé démarré")
            elif mode == 'multi' and MULTI_SESSION_AVAILABLE:
                click.echo("✅ Système multi-session démarré")
            else:
                click.echo("✅ Système de base démarré")
        else:
            click.echo("❌ Échec du démarrage")
    
    asyncio.run(_start())

@system.command('status')
@click.option('--detailed', is_flag=True, help='Affichage détaillé')
@click.pass_context
def system_status(ctx, detailed):
    """Statut complet du système"""
    trading_cli = ctx.obj['cli']
    
    click.echo("📊 STATUT SYSTÈME TRADINGCLI V4")
    click.echo("=" * 50)
    
    # Statut des composants
    click.echo("🔧 COMPOSANTS:")
    click.echo(f"  Base system: {'✅' if trading_cli.session_manager else '❌'}")
    click.echo(f"  Workflow avancé: {'✅' if trading_cli.workflow_running else '❌'}")
    click.echo(f"  Multi-session: {'✅' if MULTI_SESSION_AVAILABLE else '❌'}")
    
    # Configuration
    click.echo(f"\n📋 CONFIGURATION:")
    click.echo(f"  Stratégies: {len(trading_cli.strategies_registry)}")
    click.echo(f"  Sessions: {len(trading_cli.sessions_registry)}")
    click.echo(f"  Profils risque: {len(trading_cli.risk_profiles_registry)}")
    
    if detailed and trading_cli.workflow_running:
        async def _detailed_status():
            global_metrics = trading_cli.strategy_manager_enhanced.get_global_metrics()
            active_strategies = trading_cli.strategy_manager_enhanced.get_active_strategies()
            
            click.echo(f"\n🚀 WORKFLOW DÉTAILLÉ:")
            click.echo(f"  Stratégies actives: {len(active_strategies)}")
            click.echo(f"  Candles traitées: {global_metrics.get('total_candles_processed', 0)}")
            click.echo(f"  Signaux générés: {global_metrics.get('total_signals_generated', 0)}")
            click.echo(f"  Uptime: {global_metrics.get('uptime_seconds', 0):.1f}s")
        
        asyncio.run(_detailed_status())

@system.command('metrics')
@click.option('--export', help='Exporter vers fichier JSON')
@click.pass_context
def system_metrics(ctx, export):
    """Métriques système détaillées"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.workflow_running:
        click.echo("⚠️ Workflow non démarré")
        return
    
    async def _get_metrics():
        global_metrics = trading_cli.strategy_manager_enhanced.get_global_metrics()
        
        click.echo("📊 MÉTRIQUES SYSTÈME COMPLÈTES")
        click.echo("=" * 50)
        
        # Métriques globales
        click.echo("🌐 GLOBALES:")
        for key, value in global_metrics.items():
            if key != 'indicators_service_metrics':
                click.echo(f"  {key}: {value}")
        
        # Métriques des indicateurs
        indicators_metrics = global_metrics.get('indicators_service_metrics', {})
        click.echo("\n🔧 INDICATEURS:")
        for key, value in indicators_metrics.items():
            click.echo(f"  {key}: {value}")
        
        if export:
            with open(export, 'w') as f:
                json.dump(global_metrics, f, indent=2, default=str)
            click.echo(f"\n💾 Métriques exportées vers {export}")
    
    asyncio.run(_get_metrics())

# ============================================================================
# COMMANDES SESSIONS (Fusion CLI de base + V2 + Adapted)
# ============================================================================

@cli.group()
def session():
    """📅 Gestion avancée des sessions"""
    pass

@session.command('create')
@click.option('--name', prompt='Nom de la session', help='Nom de la session')
@click.option('--mode', 
              type=click.Choice(['sandbox', 'paper', 'live']),
              prompt='Mode de trading',
              help='Mode de trading')
@click.option('--no-workflow', is_flag=True, help='Désactiver le workflow avancé (activé par défaut)')
@click.option('--workflow', is_flag=True, help='[DEPRECATED] Utiliser --no-workflow pour désactiver')
@click.option('--initial-balance', default=10000.0, type=float, help='Balance initiale')
@click.option('--count', default=1, type=int, help='Nombre de sessions à créer')
@click.pass_context
def create_session(ctx, name, mode, no_workflow, workflow, initial_balance, count):
    """Créer une ou plusieurs sessions"""
    trading_cli = ctx.obj['cli']
    
    async def _create():
        # Déterminer si le workflow est activé (par défaut OUI, sauf si --no-workflow)
        workflow_enabled = not no_workflow  # Workflow par défaut, désactivé seulement avec --no-workflow
        
        # Support backward compatibility pour --workflow
        if workflow:
            workflow_enabled = True
            click.echo("⚠️ Option --workflow deprecated, workflow activé par défaut maintenant")
        
        if not trading_cli.session_manager:
            await trading_cli.initialize_system()
        
        created_sessions = []
        
        for i in range(count):
            session_name = name if count == 1 else f"{name}_{i+1}"
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}" if count > 1 else f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Configuration de session
            session_config = {
                'id': session_id,
                'name': session_name,
                'mode': mode,
                'workflow_enabled': workflow_enabled,
                'strategies': [],
                'initial_balance': initial_balance,
                'status': 'created',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Sauvegarder dans le registre
            trading_cli.sessions_registry[session_id] = session_config
            
            # Créer dans la DB aussi (compatibilité)
            try:
                strategy_config = StrategyConfig(
                    name=session_name,
                    parameters={'initial_balance': initial_balance},
                    timeframe='5m',
                    pairs=['BTCUSD']
                )
                
                session_mode = SessionMode.SANDBOX if mode == 'sandbox' else SessionMode.PAPER if mode == 'paper' else SessionMode.LIVE
                
                await trading_cli.session_manager.create_session(
                    session_id, strategy_config, session_mode
                )
                
                created_sessions.append(session_id)
                
            except Exception as e:
                click.echo(f"⚠️ Erreur DB pour {session_id}: {e}")
        
        trading_cli._save_configuration()
        
        if created_sessions:
            click.echo(f"✅ {len(created_sessions)} session(s) créée(s)")
            for sid in created_sessions:
                click.echo(f"  - {sid}")
            
            if workflow_enabled:
                click.echo("🚀 Sessions configurées pour le workflow avancé")
            else:
                click.echo("⚪ Sessions configurées en mode standard (workflow désactivé)")
    
    asyncio.run(_create())

@session.command('list')
@click.option('--mode', type=click.Choice(['all', 'running', 'stopped']), 
              default='all', help='Filtre par statut')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json', 'yaml']), 
              default='table', help='Format de sortie')
@click.pass_context
def list_sessions(ctx, mode, output_format):
    """Lister les sessions avec filtres"""
    trading_cli = ctx.obj['cli']
    
    sessions_data = []
    
    # Combiner registre et DB
    for session_id, session in trading_cli.sessions_registry.items():
        status = session.get('status', 'unknown')
        
        if mode == 'running' and status != 'running':
            continue
        elif mode == 'stopped' and status == 'running':
            continue
        
        sessions_data.append([
            session_id[:15] + "..." if len(session_id) > 15 else session_id,
            session['name'][:20] + "..." if len(session['name']) > 20 else session['name'],
            session['mode'],
            len(session.get('strategies', [])),
            status,
            '🚀' if session.get('workflow_enabled') else '⚪',
            session.get('created_at', 'N/A')[:10]
        ])
    
    if output_format == 'table':
        headers = ['ID', 'Nom', 'Mode', 'Stratégies', 'Status', 'Workflow', 'Créé']
        click.echo(tabulate(sessions_data, headers=headers, tablefmt='grid'))
    
    elif output_format == 'json':
        click.echo(json.dumps(trading_cli.sessions_registry, indent=2, default=str))
    
    elif output_format == 'yaml':
        click.echo(yaml.dump(trading_cli.sessions_registry, default_flow_style=False))

@session.command('start')
@click.argument('session_id')
@click.option('--force', is_flag=True, help='Forcer le démarrage')
@click.pass_context
def start_session(ctx, session_id, force):
    """Démarrer une session (workflow ou standard)"""
    trading_cli = ctx.obj['cli']
    
    async def _start():
        if session_id not in trading_cli.sessions_registry:
            click.echo(f"❌ Session '{session_id}' introuvable!")
            return
        
        session = trading_cli.sessions_registry[session_id]
        
        try:
            # Démarrage workflow si activé
            if session.get('workflow_enabled') and WORKFLOW_AVAILABLE:
                if not trading_cli.workflow_running:
                    await trading_cli.initialize_system()
                
                # Démarrer les stratégies dans le workflow
                success_count = 0
                for strategy_id in session.get('strategies', []):
                    if strategy_id in trading_cli.strategies_registry:
                        strategy_data = trading_cli.strategies_registry[strategy_id]
                        enhanced_config = trading_cli.convert_to_enhanced_config(strategy_data)
                        
                        base_config = StrategyConfig(
                            name=enhanced_config.name,
                            parameters=enhanced_config.parameters,
                            timeframe=enhanced_config.timeframe,
                            pairs=enhanced_config.pairs
                        )
                        
                        success = await trading_cli.strategy_manager_enhanced.start_strategy(
                            strategy_id, base_config
                        )
                        if success:
                            success_count += 1
                
                session['status'] = 'running'
                session['start_time'] = datetime.now(timezone.utc).isoformat()
                trading_cli._save_configuration()
                
                click.echo(f"🚀 Session workflow '{session['name']}' démarrée")
                click.echo(f"📊 {success_count} stratégies actives")
            
            else:
                # Démarrage standard
                if not trading_cli.session_manager:
                    await trading_cli.initialize_system()
                
                await trading_cli.session_manager.start_session(session_id)
                session['status'] = 'running'
                session['start_time'] = datetime.now(timezone.utc).isoformat()
                trading_cli._save_configuration()
                
                click.echo(f"✅ Session '{session['name']}' démarrée")
        
        except Exception as e:
            click.echo(f"❌ Erreur lors du démarrage: {e}")
    
    asyncio.run(_start())

@session.command('show')
@click.argument('session_id')
@click.option('--live', is_flag=True, help='Mode temps réel')
@click.pass_context
def show_session(ctx, session_id, live):
    """Afficher les détails complets d'une session"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable!")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    def display_session_info():
        click.echo(f"📋 Détails de la session '{session_id}':")
        click.echo(f"  Nom: {session['name']}")
        click.echo(f"  Mode: {session['mode']}")
        click.echo(f"  Status: {session.get('status', 'unknown')}")
        click.echo(f"  Workflow: {'🚀 Activé' if session.get('workflow_enabled') else '⚪ Standard'}")
        click.echo(f"  Balance initiale: {session.get('initial_balance', 'N/A')}€")
        click.echo(f"  Créé le: {session.get('created_at', 'N/A')}")
        
        if session.get('start_time'):
            click.echo(f"  Démarré le: {session['start_time']}")
        
        click.echo(f"\n🎯 Stratégies ({len(session.get('strategies', []))}):")
        for strategy_id in session.get('strategies', []):
            if strategy_id in trading_cli.strategies_registry:
                strategy = trading_cli.strategies_registry[strategy_id]
                status_icon = "🟢" if strategy.get('status') == 'active' else "⚪"
                click.echo(f"  {status_icon} {strategy_id}: {strategy['name']}")
        
        # Performance temps réel si workflow actif
        if trading_cli.workflow_running and session.get('workflow_enabled'):
            async def _show_realtime():
                try:
                    active_strategies = trading_cli.strategy_manager_enhanced.get_active_strategies()
                    global_metrics = trading_cli.strategy_manager_enhanced.get_global_metrics()
                    
                    click.echo(f"\n🚀 PERFORMANCE TEMPS RÉEL:")
                    click.echo(f"  Stratégies actives: {len(active_strategies)}")
                    click.echo(f"  Candles traitées: {global_metrics.get('total_candles_processed', 0)}")
                    click.echo(f"  Signaux générés: {global_metrics.get('total_signals_generated', 0)}")
                    
                    for strategy_id in session.get('strategies', []):
                        if strategy_id in active_strategies:
                            status = await trading_cli.strategy_manager_enhanced.get_strategy_status(strategy_id)
                            if status:
                                click.echo(f"\n  📊 {strategy_id}:")
                                click.echo(f"    Queue: {status['queue_size']}")
                                click.echo(f"    DataFrame: {status['dataframe_info'].get('rows', 0)} rows")
                                click.echo(f"    Métriques: {status['metrics']}")
                
                except Exception as e:
                    click.echo(f"⚠️ Erreur temps réel: {e}")
            
            asyncio.run(_show_realtime())
    
    if live:
        # Mode temps réel avec rafraîchissement
        try:
            while True:
                os.system('clear' if os.name == 'posix' else 'cls')
                click.echo(f"🔄 Mode temps réel - {datetime.now().strftime('%H:%M:%S')} (Ctrl+C pour quitter)")
                click.echo("=" * 60)
                display_session_info()
                time.sleep(2)
        except KeyboardInterrupt:
            click.echo("\n✅ Mode temps réel arrêté")
    else:
        display_session_info()

# ============================================================================
# COMMANDES ANALYTICS (V2 + améliorations)
# ============================================================================

@cli.group()
def analytics():
    """📊 Analytics et rapports avancés"""
    pass

@analytics.command('portfolio')
@click.option('--export', help='Exporter vers fichier')
@click.option('--period', default=30, type=int, help='Période en jours')
@click.pass_context
def portfolio_report(ctx, export, period):
    """Rapport de portfolio complet"""
    trading_cli = ctx.obj['cli']
    
    click.echo(f"📊 RAPPORT PORTFOLIO ({period} derniers jours)")
    click.echo("=" * 60)
    
    # Analyser toutes les sessions
    portfolio_data = {
        'total_sessions': len(trading_cli.sessions_registry),
        'active_sessions': len([s for s in trading_cli.sessions_registry.values() if s.get('status') == 'running']),
        'total_strategies': len(trading_cli.strategies_registry),
        'workflow_sessions': len([s for s in trading_cli.sessions_registry.values() if s.get('workflow_enabled')]),
        'generated_at': datetime.now(timezone.utc).isoformat()
    }
    
    # Performance par mode
    modes_performance = {}
    for session in trading_cli.sessions_registry.values():
        mode = session['mode']
        if mode not in modes_performance:
            modes_performance[mode] = {'count': 0, 'active': 0}
        modes_performance[mode]['count'] += 1
        if session.get('status') == 'running':
            modes_performance[mode]['active'] += 1
    
    click.echo("📈 RÉSUMÉ PORTFOLIO:")
    click.echo(f"  Sessions totales: {portfolio_data['total_sessions']}")
    click.echo(f"  Sessions actives: {portfolio_data['active_sessions']}")
    click.echo(f"  Stratégies configurées: {portfolio_data['total_strategies']}")
    click.echo(f"  Sessions workflow: {portfolio_data['workflow_sessions']}")
    
    click.echo(f"\n🎯 RÉPARTITION PAR MODE:")
    for mode, data in modes_performance.items():
        click.echo(f"  {mode.upper()}: {data['count']} total, {data['active']} actives")
    
    if export:
        portfolio_data['modes_performance'] = modes_performance
        with open(export, 'w') as f:
            json.dump(portfolio_data, f, indent=2, default=str)
        click.echo(f"\n💾 Rapport exporté vers {export}")

@analytics.command('compare')
@click.argument('session_ids', nargs=-1, required=True)
@click.option('--metric', default='performance', help='Métrique de comparaison')
@click.pass_context
def compare_sessions(ctx, session_ids, metric):
    """Comparaison avancée entre sessions"""
    trading_cli = ctx.obj['cli']
    
    click.echo(f"🔍 COMPARAISON DE {len(session_ids)} SESSIONS")
    click.echo("=" * 50)
    
    comparison_data = []
    
    for session_id in session_ids:
        if session_id in trading_cli.sessions_registry:
            session = trading_cli.sessions_registry[session_id]
            
            comparison_data.append([
                session_id[:12] + "..." if len(session_id) > 12 else session_id,
                session['name'][:15] + "..." if len(session['name']) > 15 else session['name'],
                session['mode'],
                len(session.get('strategies', [])),
                '🚀' if session.get('workflow_enabled') else '⚪',
                session.get('status', 'unknown'),
                session.get('initial_balance', 0)
            ])
        else:
            click.echo(f"⚠️ Session '{session_id}' introuvable")
    
    if comparison_data:
        headers = ['ID', 'Nom', 'Mode', 'Stratégies', 'Workflow', 'Status', 'Balance Init.']
        click.echo(tabulate(comparison_data, headers=headers, tablefmt='grid'))

# ============================================================================
# COMMANDES STRATÉGIES (Fusion V3 + améliorations)
# ============================================================================

@cli.group()
def strategy():
    """🎯 Gestion avancée des stratégies"""
    pass

@strategy.command('list')
@click.option('--filter-workflow', is_flag=True, help='Seulement les stratégies workflow')
@click.option('--filter-active', is_flag=True, help='Seulement les stratégies actives')
@click.pass_context
def list_strategies(ctx, filter_workflow, filter_active):
    """Lister les stratégies avec filtres"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.strategies_registry:
        click.echo("Aucune stratégie configurée.")
        return
    
    table_data = []
    for strategy_id, strategy_config in trading_cli.strategies_registry.items():
        # Filtres
        if filter_workflow and not strategy_config.get('workflow_enabled'):
            continue
        if filter_active and strategy_config.get('status') != 'active':
            continue
        
        # Statut avancé
        advanced_status = "N/A"
        if trading_cli.workflow_running:
            active_strategies = trading_cli.strategy_manager_enhanced.get_active_strategies()
            if strategy_id in active_strategies:
                advanced_status = "🟢 ACTIVE"
            else:
                advanced_status = "⚪ CONFIGURED"
        
        table_data.append([
            strategy_id[:15] + "..." if len(strategy_id) > 15 else strategy_id,
            strategy_config.get('name', 'N/A')[:20] + "..." if len(strategy_config.get('name', '')) > 20 else strategy_config.get('name', 'N/A'),
            strategy_config.get('symbol', 'N/A'),
            strategy_config.get('timeframe', 'N/A'),
            len(strategy_config.get('indicators', {})),
            '🚀' if strategy_config.get('workflow_enabled') else '⚪',
            advanced_status
        ])
    
    headers = ['ID', 'Nom', 'Paire', 'Timeframe', 'Indicateurs', 'Workflow', 'Status']
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))

@strategy.command('create')
@click.option('--name', prompt='Nom de la stratégie', help='Nom de la stratégie')
@click.option('--id', 'strategy_id', help='ID unique (auto si non spécifié)')
@click.option('--no-workflow', is_flag=True, help='Désactiver le workflow avancé (activé par défaut)')
@click.option('--workflow', is_flag=True, help='[DEPRECATED] Utiliser --no-workflow pour désactiver')
@click.option('--interactive', is_flag=True, help='Configuration interactive complète')
@click.pass_context
def create_strategy(ctx, name, strategy_id, no_workflow, workflow, interactive):
    """Créer une stratégie (standard ou workflow)"""
    trading_cli = ctx.obj['cli']
    
    # Déterminer si le workflow est activé (par défaut OUI, sauf si --no-workflow)
    workflow_enabled = not no_workflow  # Workflow par défaut, désactivé seulement avec --no-workflow
    
    # Support backward compatibility pour --workflow
    if workflow:
        workflow_enabled = True
        click.echo("⚠️ Option --workflow deprecated, workflow activé par défaut maintenant")
    
    # Générer ID si non fourni
    if not strategy_id:
        strategy_id = name.lower().replace(' ', '_').replace('-', '_')
        if strategy_id in trading_cli.strategies_registry:
            strategy_id += f"_{datetime.now().strftime('%H%M%S')}"
    
    if strategy_id in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' existe déjà!")
        return
    
    # Configuration de base
    strategy_config = {
        'id': strategy_id,
        'name': name,
        'symbol': None,
        'timeframe': None,
        'parameters': {},
        'indicators': {},
        'signal_rules': {
            'long_condition': None,
            'short_condition': None
        },
        'risk_profile': 'default',
        'start_date': None,
        'end_date': 'now',
        'status': 'inactive',
        'workflow_enabled': workflow_enabled,
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    
    if interactive:
        # Configuration interactive complète
        click.echo(f"🎯 Configuration interactive de '{name}'")
        
        symbol = click.prompt('Paire de trading', default='BTCUSD')
        strategy_config['symbol'] = symbol
        
        timeframe_choices = ['1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d']
        click.echo(f"Timeframes: {', '.join(timeframe_choices)}")
        timeframe = click.prompt('Timeframe', default='5m')
        strategy_config['timeframe'] = timeframe
        
        if workflow_enabled and WORKFLOW_AVAILABLE:
            click.echo("\n🚀 Configuration workflow avancé:")
            short_window = click.prompt('Fenêtre courte pour SMA', default=10, type=int)
            long_window = click.prompt('Fenêtre longue pour SMA', default=30, type=int)
            
            strategy_config['parameters'] = {
                'short_window': short_window,
                'long_window': long_window
            }
            
            # Ajouter indicateurs automatiques
            strategy_config['indicators'] = {
                f'SMA_{short_window}': {
                    'type': 'SMA',
                    'parameters': {'period': short_window},
                    'source': 'close'
                },
                f'SMA_{long_window}': {
                    'type': 'SMA',
                    'parameters': {'period': long_window},
                    'source': 'close'
                },
                'RSI_14': {
                    'type': 'RSI',
                    'parameters': {'period': 14},
                    'source': 'close'
                }
            }
        
        # Configuration Risk Management
        if click.confirm('Configurer le Risk Management?', default=True):
            click.echo("\n🛡️ Configuration Risk Management:")
            
            max_pos_size = click.prompt('Position size max (0.01-1.0)', default=0.1, type=float)
            stop_loss = click.prompt('Stop loss (%)', default=2.0, type=float)
            take_profit = click.prompt('Take profit (%)', default=4.0, type=float)
            risk_ratio = click.prompt('Risk Ratio (1:RR) - ex: 2.0 = 1:2', default=2.0, type=float)
            max_daily_loss = click.prompt('Perte journalière max (€)', default=500.0, type=float)
            max_trades = click.prompt('Trades simultanés max', default=2, type=int)
            
            strategy_config['risk_management'] = {
                'max_position_size': max_pos_size,
                'stop_loss_percent': stop_loss,
                'take_profit_percent': take_profit,
                'risk_ratio': risk_ratio,
                'max_daily_loss': max_daily_loss,
                'max_concurrent_trades': max_trades,
                'position_size_method': 'percent'
            }
    
    trading_cli.strategies_registry[strategy_id] = strategy_config
    trading_cli._save_configuration()
    
    click.echo(f"✅ Stratégie '{name}' créée avec l'ID '{strategy_id}'")
    if workflow_enabled:
        click.echo("🚀 Mode workflow avancé activé")
    else:
        click.echo("⚪ Mode standard (workflow désactivé)")
    if interactive:
        click.echo("💡 Configuration interactive terminée")
    else:
        click.echo("💡 Utilisez 'strategy configure' pour configurer les paramètres")

@strategy.command('show')
@click.argument('strategy_id')
@click.pass_context
def show_strategy(ctx, strategy_id):
    """Afficher les détails complets d'une stratégie"""
    trading_cli = ctx.obj['cli']
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    strategy = trading_cli.strategies_registry[strategy_id]
    
    click.echo(f"📋 Détails de la stratégie '{strategy_id}':")
    click.echo(f"  Nom: {strategy['name']}")
    click.echo(f"  Paire: {strategy.get('symbol', 'Non défini')}")
    click.echo(f"  Timeframe: {strategy.get('timeframe', 'Non défini')}")
    click.echo(f"  Date début: {strategy.get('start_date', 'Non défini')}")
    click.echo(f"  Date fin: {strategy.get('end_date', 'now')}")
    click.echo(f"  Risk Profile: {strategy.get('risk_profile', 'default')}")
    click.echo(f"  Status: {strategy.get('status', 'inactive')}")
    click.echo(f"  Workflow: {'🚀 Activé' if strategy.get('workflow_enabled') else '❌ Désactivé'}")
    
    # Migration info
    if strategy.get('migrated_from'):
        click.echo(f"  Migré depuis: {strategy['migrated_from']} le {strategy.get('migration_date', 'N/A')[:10]}")
    
    # Paramètres V4
    if strategy.get('v4_features'):
        click.echo(f"\n🚀 Fonctionnalités V4:")
        for feature, enabled in strategy['v4_features'].items():
            status = "✅" if enabled else "❌"
            click.echo(f"    {feature}: {status}")
    
    # Paramètres du workflow
    if strategy.get('parameters'):
        click.echo(f"\n⚙️ Paramètres:")
        for param, value in strategy['parameters'].items():
            click.echo(f"  {param}: {value}")
    
    click.echo("\n📈 Signaux:")
    click.echo(f"  LONG: {strategy['signal_rules'].get('long_condition', 'Non défini')}")
    click.echo(f"  SHORT: {strategy['signal_rules'].get('short_condition', 'Non défini')}")
    
    # Risk Management de la stratégie
    if strategy.get('risk_management'):
        risk_mgmt = strategy['risk_management']
        click.echo(f"\n🛡️ Risk Management:")
        click.echo(f"  Position size: {risk_mgmt.get('max_position_size', 'N/A')}")
        click.echo(f"  Stop loss: {risk_mgmt.get('stop_loss_percent', 'N/A')}%")
        click.echo(f"  Take profit: {risk_mgmt.get('take_profit_percent', 'N/A')}%")
        click.echo(f"  Risk Ratio (RR): 1:{risk_mgmt.get('risk_ratio', 'N/A')}")
        click.echo(f"  Max daily loss: {risk_mgmt.get('max_daily_loss', 'N/A')}€")
        click.echo(f"  Max concurrent trades: {risk_mgmt.get('max_concurrent_trades', 'N/A')}")
    
    click.echo(f"\n🔧 Indicateurs ({len(strategy.get('indicators', {}))}):")
    for ind_name, ind_config in strategy.get('indicators', {}).items():
        click.echo(f"  - {ind_name}: {ind_config.get('type', 'N/A')}({ind_config.get('parameters', {})})")
    
    # Si le workflow est activé, simuler la configuration EnhancedStrategyConfig
    if strategy.get('workflow_enabled') and WORKFLOW_AVAILABLE:
        try:
            enhanced = trading_cli.convert_to_enhanced_config(strategy)
            click.echo(f"\n🚀 Configuration workflow avancé:")
            click.echo(f"  Indicateurs auto-configurés: {len(enhanced.indicators)}")
            for ind_name, ind_config in enhanced.indicators.items():
                click.echo(f"    - {ind_name}: {ind_config.type}({ind_config.parameters})")
            
            click.echo(f"  Règles de signaux: {len(enhanced.signal_rules)}")
            for rule in enhanced.signal_rules:
                click.echo(f"    - {rule.name}: {rule.condition}")
        
        except Exception as e:
            click.echo(f"⚠️ Erreur lors de la simulation du workflow: {e}")

# ============================================================================
# COMMANDES TEST (V3 + Multi-session)
# ============================================================================

@cli.group()
def test():
    """🧪 Tests et validation"""
    pass

@test.command('workflow')
@click.option('--duration', default=30, type=int, help='Durée du test en secondes')
@click.option('--strategies', default=1, type=int, help='Nombre de stratégies de test')
@click.pass_context
def test_workflow(ctx, duration, strategies):
    """Test complet du workflow avec métriques"""
    trading_cli = ctx.obj['cli']
    
    if not WORKFLOW_AVAILABLE:
        click.echo("❌ Workflow non disponible")
        return
    
    async def _run_test():
        click.echo(f"🧪 Test workflow - {strategies} stratégie(s) pendant {duration}s")
        
        # Initialiser le système
        await trading_cli.initialize_system()
        
        # Créer stratégies de test
        test_strategies = []
        for i in range(strategies):
            strategy_name = f"TestStrategy_{i+1}"
            
            test_strategy_config = {
                'name': strategy_name,
                'symbol': 'BTCUSD',
                'timeframe': '5m',
                'parameters': {'short_window': 3 + i, 'long_window': 5 + i * 2},
                'indicators': {},
                'signal_rules': {
                    'long_condition': f'SMA_{3+i} > SMA_{5+i*2} & RSI_14 < 70',
                    'short_condition': f'SMA_{3+i} < SMA_{5+i*2} & RSI_14 > 30'
                },
                'workflow_enabled': True
            }
            
            enhanced_config = trading_cli.convert_to_enhanced_config(test_strategy_config)
            base_config = StrategyConfig(
                name=enhanced_config.name,
                parameters=enhanced_config.parameters,
                timeframe=enhanced_config.timeframe,
                pairs=enhanced_config.pairs
            )
            
            session_id = f"test_strategy_{i+1}"
            success = await trading_cli.strategy_manager_enhanced.start_strategy(session_id, base_config)
            
            if success:
                test_strategies.append(session_id)
        
        click.echo(f"✅ {len(test_strategies)} stratégies de test démarrées")
        
        # Simuler données de marché
        from examples.strategy_workflow_example import MockMarketDataGenerator
        
        market_generator = MockMarketDataGenerator(trading_cli.pubsub)
        generation_task = asyncio.create_task(market_generator.start_generating(interval=1.0))
        
        # Monitoring
        start_time = asyncio.get_event_loop().time()
        last_report = 0
        
        click.echo(f"📈 Génération de données et monitoring...")
        
        while asyncio.get_event_loop().time() - start_time < duration:
            current_time = asyncio.get_event_loop().time()
            
            if current_time - last_report >= 5:
                global_metrics = trading_cli.strategy_manager_enhanced.get_global_metrics()
                elapsed = current_time - start_time
                
                click.echo(f"⏱️ {elapsed:.0f}s - Candles: {global_metrics.get('total_candles_processed', 0)}, "
                          f"Indicateurs: {global_metrics.get('indicators_service_metrics', {}).get('calculations_performed', 0)}, "
                          f"Signaux: {global_metrics.get('total_signals_generated', 0)}")
                
                last_report = current_time
            
            await asyncio.sleep(1)
        
        # Résultats finaux
        market_generator.stop_generating()
        generation_task.cancel()
        
        final_metrics = trading_cli.strategy_manager_enhanced.get_global_metrics()
        
        click.echo(f"\n🎉 Test terminé!")
        click.echo(f"📊 Résultats globaux:")
        click.echo(f"  Stratégies testées: {len(test_strategies)}")
        click.echo(f"  Candles traitées: {final_metrics.get('total_candles_processed', 0)}")
        click.echo(f"  Indicateurs calculés: {final_metrics.get('indicators_service_metrics', {}).get('calculations_performed', 0)}")
        click.echo(f"  Signaux générés: {final_metrics.get('total_signals_generated', 0)}")
        click.echo(f"  Erreurs: {final_metrics.get('total_errors', 0)}")
        
        # Nettoyage
        for strategy_id in test_strategies:
            await trading_cli.strategy_manager_enhanced.stop_strategy(strategy_id)
        
        await trading_cli.shutdown_system()
        
        click.echo("✅ Test terminé avec succès!")
    
    asyncio.run(_run_test())

@test.command('multi-session')
@click.option('--count', default=3, type=int, help='Nombre de sessions')
@click.option('--duration', default=20, type=int, help='Durée en secondes')
@click.pass_context
def test_multi_session(ctx, count, duration):
    """Test multi-session avec performance"""
    trading_cli = ctx.obj['cli']
    
    async def _test_multi():
        click.echo(f"🧪 Test multi-session - {count} sessions pendant {duration}s")
        
        await trading_cli.initialize_system()
        
        # Créer plusieurs sessions de test
        test_sessions = []
        for i in range(count):
            session_id = f"multi_test_{i+1}"
            session_config = {
                'id': session_id,
                'name': f"Multi Test Session {i+1}",
                'mode': 'sandbox',
                'workflow_enabled': True,
                'strategies': [],
                'status': 'created',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            trading_cli.sessions_registry[session_id] = session_config
            test_sessions.append(session_id)
        
        click.echo(f"✅ {len(test_sessions)} sessions de test créées")
        
        # Simuler activité
        start_time = time.time()
        while time.time() - start_time < duration:
            click.echo(f"⏱️ {time.time() - start_time:.0f}s - Sessions actives: {len(test_sessions)}")
            await asyncio.sleep(2)
        
        # Nettoyage
        for session_id in test_sessions:
            if session_id in trading_cli.sessions_registry:
                del trading_cli.sessions_registry[session_id]
        
        trading_cli._save_configuration()
        
        click.echo("✅ Test multi-session terminé!")
    
    asyncio.run(_test_multi())

# ============================================================================
# COMMANDES UTILITAIRES
# ============================================================================

@cli.command('version')
def version():
    """Afficher la version et les capacités"""
    click.echo("🚀 TradingCLI V4 - Interface Unifiée Complète")
    click.echo("=" * 50)
    click.echo("Features fusionnées:")
    click.echo("  📅 Gestion avancée des sessions (base + V2 + adapted)")
    click.echo("  🎯 Stratégies avec workflow temps réel (V3) - ACTIVÉ PAR DÉFAUT")
    click.echo("  📊 Analytics et rapports complets (V2)")
    click.echo("  💾 Configuration YAML persistante (adapted)")
    click.echo("  🔧 Système multi-composants unifié")
    
    click.echo(f"\n⚡ WORKFLOW PAR DÉFAUT:")
    click.echo(f"  Le workflow avancé est maintenant activé automatiquement")
    click.echo(f"  Utilisez --no-workflow pour le désactiver si nécessaire")
    
    click.echo(f"\nCapacités détectées:")
    click.echo(f"  Workflow avancé: {'✅' if WORKFLOW_AVAILABLE else '❌'}")
    click.echo(f"  Multi-session: {'✅' if MULTI_SESSION_AVAILABLE else '❌'}")
    click.echo(f"  Analytics: ✅")
    click.echo(f"  Configuration YAML: ✅")

@cli.command('init')
@click.option('--force', is_flag=True, help='Forcer la réinitialisation')
@click.pass_context
def init_system(ctx, force):
    """Initialiser le système complet"""
    trading_cli = ctx.obj['cli']
    
    if os.path.exists(trading_cli.config_path) and not force:
        click.echo("⚠️ Configuration existante détectée")
        if not click.confirm("Voulez-vous la réinitialiser?"):
            return
    
    # Créer configuration par défaut
    default_config = {
        'strategies': {
            'demo_unified': {
                'id': 'demo_unified',
                'name': 'Demo Unified Strategy',
                'symbol': 'BTCUSD',
                'timeframe': '5m',
                'parameters': {'short_window': 10, 'long_window': 30},
                'indicators': {
                    'SMA_10': {'type': 'SMA', 'parameters': {'period': 10}, 'source': 'close'},
                    'SMA_30': {'type': 'SMA', 'parameters': {'period': 30}, 'source': 'close'},
                    'RSI_14': {'type': 'RSI', 'parameters': {'period': 14}, 'source': 'close'}
                },
                'signal_rules': {
                    'long_condition': 'SMA_10 > SMA_30 & SMA_10[-1] <= SMA_30[-1] & RSI_14 < 70',
                    'short_condition': 'SMA_10 < SMA_30 & SMA_10[-1] >= SMA_30[-1] & RSI_14 > 30'
                },
                'workflow_enabled': True,
                'status': 'configured',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        },
        'sessions': {
            'demo_unified_session': {
                'id': 'demo_unified_session',
                'name': 'Demo Unified Session',
                'mode': 'sandbox',
                'workflow_enabled': True,
                'strategies': ['demo_unified'],
                'initial_balance': 10000.0,
                'status': 'configured',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        },
        'risk_profiles': {
            'default': {
                'name': 'Profil par défaut',
                'position_size_value': 2.0,
                'max_concurrent_trades': 3,
                'stop_loss_percent': 2.0,
                'take_profit_percent': 4.0
            }
        }
    }
    
    # Sauvegarder
    trading_cli.strategies_registry = default_config['strategies']
    trading_cli.sessions_registry = default_config['sessions']
    trading_cli.risk_profiles_registry = default_config['risk_profiles']
    trading_cli._save_configuration()
    
    click.echo("✅ Système initialisé avec configuration par défaut")
    click.echo("💡 Utilisez 'session list' pour voir les sessions disponibles")
    click.echo("🚀 Utilisez 'test workflow' pour tester le système")

if __name__ == '__main__':
    cli()