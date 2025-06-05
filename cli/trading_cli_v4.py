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

# Imports smart filters

from utils.smart_filters import (
    smart_resolve_ids, SessionFilter, StrategyFilter,
    UniversalFilter, smart_resolve_params
)
SMART_FILTERS_AVAILABLE = True
click.echo("⚠️ Smart filters disponibles")

# Imports workflow avancé
try:
    from core.strategy_workflow import TradingStrategy, IndicatorConfig, SignalRule
    #from core.indicators_service import IndicatorsService
    from services.indicators_service import IndicatorsService
    from services.candles_service import CandlesService
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

# Aussi, ajouter ces imports en haut du fichier trading_cli_v4.py (après les autres imports)
try:
    from grid_generator import StrategyGridGenerator

    GRID_GENERATOR_AVAILABLE = True
except ImportError:
    GRID_GENERATOR_AVAILABLE = False
    click.echo("⚠️ Générateur de grid non disponible")

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
        self.risk_profils_registry = {}
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
                    self.risk_profils_registry = config.get('risk_profils', {})
                    self.sessions_registry = config.get('sessions', {})
            except Exception as e:
                click.echo(f"⚠️ Erreur de chargement config: {e}")
                # En cas d'erreur, initialiser avec des dictionnaires vides
                self.risk_profils_registry = {}
                self.strategies_registry = {}
                self.indicators_registry = {}
                self.sessions_registry = {}
    
    def _save_configuration(self):
        """Sauvegarde la configuration (CLI Adapted)"""
        config = {
            'strategies': self.strategies_registry,
            'indicators': self.indicators_registry,
            'risk_profils': self.risk_profils_registry,
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
        if 'default' not in self.risk_profils_registry:
            self.risk_profils_registry['default'] = {
                'name': 'Profil par défaut',
                'position_size_method': 'percent',
                'position_size_value': 2.0,
                'max_concurrent_trades': 3,
                'max_daily_loss': 500.0,
                'stop_loss_percent': 2.0,
                'take_profit_percent': 4.0,
                'trailing_stop': False,
                'RR': 1.5
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
        
        base_config = StrategyConfig(
            name=strategy_data['name'],
            parameters=strategy_data.get('parameters', {}),
            timeframe=strategy_data.get('timeframe', '5m'),
            pairs=[strategy_data.get('symbol', 'BTCUSDC')],
            since=strategy_data.get('since', '2025-01-01 00:00:00'),
            to=strategy_data.get('now', 'now'),
            risk_config=None,
            initial_balance=strategy_data.get('initial_balance', 100.0),
            current_balance=strategy_data.get('current_balance')
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

# Variable globale pour activer/désactiver les traces
_TRACE_ENABLED = False

def enable_trace():
    """Active le mode trace"""
    global _TRACE_ENABLED
    _TRACE_ENABLED = True

def disable_trace():
    """Désactive le mode trace"""
    global _TRACE_ENABLED
    _TRACE_ENABLED = False

def is_trace_enabled():
    """Vérifie si le mode trace est activé"""
    return _TRACE_ENABLED

def trace_method(func_name: str, args: tuple = None, kwargs: dict = None):
    """Log des appels de méthodes si trace activé"""
    if not _TRACE_ENABLED:
        return
    
    # Formater les arguments
    args_str = ""
    if args:
        args_str = ", ".join(str(arg) for arg in args if arg is not None)
    
    kwargs_str = ""
    if kwargs:
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
    
    params_str = ", ".join(filter(None, [args_str, kwargs_str]))
    
    click.echo(f"🔍 [TRACE] {func_name}({params_str})", err=True)

def trace_decorator(func):
    """Décorateur pour tracer automatiquement les appels de fonction"""
    import functools
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if _TRACE_ENABLED:
            # Exclure le context click des arguments affichés
            clean_args = []
            clean_kwargs = {}
            
            for arg in args:
                if not hasattr(arg, 'obj'):  # Exclure les contextes click
                    clean_args.append(arg)
            
            for k, v in kwargs.items():
                if k not in ['ctx'] and not hasattr(v, 'obj'):
                    clean_kwargs[k] = v
            
            trace_method(func.__name__, tuple(clean_args), clean_kwargs)
        
        return func(*args, **kwargs)
    return wrapper

# Commande principale
@click.group()
@click.option('--trace', is_flag=True, help='Activer le mode trace pour débugger')
@click.pass_context
def cli(ctx, trace):
    """🚀 TradingCLI V4 - Interface Unifiée Complète"""
    ctx.ensure_object(dict)
    ctx.obj['cli'] = TradingCLI()
    
    # Activer/désactiver le trace selon l'option
    if trace:
        enable_trace()
        click.echo("🔍 Mode trace activé", err=True)
    else:
        disable_trace()

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
@trace_decorator
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
@trace_decorator
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
    click.echo(f"  Profils risque: {len(trading_cli.risk_profils_registry)}")

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
@trace_decorator
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
@click.option('--initial-balance', default=0.0, type=float, help='[DEPRECATED] Balance maintenant gérée par stratégie')
@click.option('--count', default=1, type=int, help='Nombre de sessions à créer')
@click.pass_context
@trace_decorator
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
            
            # Configuration de session (balance supprimée, maintenant gérée par stratégie)
            session_config = {
                'id': session_id,
                'name': session_name,
                'mode': mode,
                'workflow_enabled': workflow_enabled,
                'strategies': [],
                'status': 'created',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Warning si balance spécifiée
            if initial_balance > 0:
                click.echo("⚠️ --initial-balance deprecated: Balance maintenant gérée par stratégie")
            
            # Sauvegarder dans le registre
            trading_cli.sessions_registry[session_id] = session_config
            
            # Créer dans la DB aussi (compatibilité)
            try:
                strategy_config = StrategyConfig(
                    name=session_name,
                    parameters={'initial_balance': initial_balance},
                    timeframe='5m',
                    pairs=['BTCUSDC'],
                    since="2025-05-01 00:00:00",
                    to="now"
                )
                
                session_mode = SessionMode.SANDBOX if mode == 'sandbox' else SessionMode.PAPER if mode == 'paper' else SessionMode.LIVE
                
                await trading_cli.session_manager.create_session(
                    session_id, session_mode, strategy_config,
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
@trace_decorator
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
@smart_resolve_params(session_id='session')
@trace_decorator
def start_session(ctx, session_id, force):
    """Démarrer une session (workflow ou standard) - utilise la recherche intelligente"""
    trading_cli = ctx.obj['cli']
    
    async def _start():
        
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
                            pairs=enhanced_config.pairs,
                            since = enhanced_config.since,
                            to = enhanced_config.to
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
@smart_resolve_params(session_id='session')
@trace_decorator
def show_session(ctx, session_id, live):
    """Afficher les détails complets d'une session (utilise la recherche intelligente)"""
    trading_cli = ctx.obj['cli']
    
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

@session.command('add-strategy')
@click.argument('session_id')
@click.argument('strategy_id')
@click.pass_context
@smart_resolve_params(session_id='session', strategy_id='strategy')
@trace_decorator
def add_strategy_to_session(ctx, session_id, strategy_id):
    """Ajouter une stratégie à une session (utilise la recherche intelligente)"""
    trading_cli = ctx.obj['cli']
    
    session = trading_cli.sessions_registry[session_id]
    strategy = trading_cli.strategies_registry[strategy_id]
    
    # Vérifications de compatibilité
    if session.get('workflow_enabled') and not strategy.get('workflow_enabled'):
        if not click.confirm(f"⚠️ Session workflow mais stratégie standard. Continuer?"):
            return
    
    if 'strategies' not in session:
        session['strategies'] = []
    
    if strategy_id not in session['strategies']:
        session['strategies'].append(strategy_id)
        trading_cli._save_configuration()
        click.echo(f"✅ Stratégie '{strategy_id}' ajoutée à la session '{session_id}'")
        click.echo(f"📊 Session contient maintenant {len(session['strategies'])} stratégie(s)")
    else:
        click.echo(f"⚠️ Stratégie '{strategy_id}' déjà dans la session!")

@session.command('remove-strategy')
@click.argument('session_id')
@click.argument('strategy_id')
@click.pass_context
@smart_resolve_params(session_id='session', strategy_id='strategy')
@trace_decorator
def remove_strategy_from_session(ctx, session_id, strategy_id):
    """Retirer une stratégie d'une session (utilise la recherche intelligente)"""
    trading_cli = ctx.obj['cli']
    
    session = trading_cli.sessions_registry[session_id]
    
    if 'strategies' not in session:
        session['strategies'] = []
    
    if strategy_id in session['strategies']:
        session['strategies'].remove(strategy_id)
        trading_cli._save_configuration()
        click.echo(f"✅ Stratégie '{strategy_id}' retirée de la session '{session_id}'")
        click.echo(f"📊 Session contient maintenant {len(session['strategies'])} stratégie(s)")
    else:
        click.echo(f"⚠️ Stratégie '{strategy_id}' n'est pas dans cette session!")

@session.command('list-strategies')
@click.argument('session_id')
@click.option('--detailed', is_flag=True, help='Affichage détaillé des stratégies')
@click.pass_context
@smart_resolve_params(session_id='session')
@trace_decorator
def list_session_strategies(ctx, session_id, detailed):
    """Lister les stratégies d'une session (utilise la recherche intelligente)"""
    trading_cli = ctx.obj['cli']
    
    session = trading_cli.sessions_registry[session_id]
    strategies = session.get('strategies', [])
    
    if not strategies:
        click.echo(f"⚪ Aucune stratégie dans la session '{session_id}'")
        return
    
    click.echo(f"🎯 Stratégies de la session '{session['name']}' ({len(strategies)}):")
    
    if detailed:
        for strategy_id in strategies:
            if strategy_id in trading_cli.strategies_registry:
                strategy = trading_cli.strategies_registry[strategy_id]
                balance = strategy.get('current_balance', strategy.get('initial_balance', 'N/A'))
                workflow_icon = "🚀" if strategy.get('workflow_enabled') else "⚪"
                click.echo(f"\n  📋 {strategy_id}:")
                click.echo(f"    Nom: {strategy['name']}")
                click.echo(f"    Paire: {strategy.get('symbol', 'N/A')}")
                click.echo(f"    Balance: {balance}€")
                click.echo(f"    Workflow: {workflow_icon}")
                click.echo(f"    Status: {strategy.get('status', 'inactive')}")
            else:
                click.echo(f"  ❌ {strategy_id} (stratégie introuvable)")
    else:
        # Affichage compact
        table_data = []
        for strategy_id in strategies:
            if strategy_id in trading_cli.strategies_registry:
                strategy = trading_cli.strategies_registry[strategy_id]
                balance = strategy.get('current_balance', strategy.get('initial_balance', 'N/A'))
                workflow_icon = "🚀" if strategy.get('workflow_enabled') else "⚪"
                table_data.append([
                    strategy_id[:20] + "..." if len(strategy_id) > 20 else strategy_id,
                    strategy['name'][:25] + "..." if len(strategy['name']) > 25 else strategy['name'],
                    strategy.get('symbol', 'N/A'),
                    f"{balance}€",
                    workflow_icon,
                    strategy.get('status', 'inactive')
                ])
            else:
                table_data.append([strategy_id, "❌ INTROUVABLE", "N/A", "N/A", "❌", "error"])
        
        headers = ['ID', 'Nom', 'Paire', 'Balance', 'Workflow', 'Status']
        click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))

@session.command('kill')
@click.argument('session_id')
@click.option('--force', is_flag=True, help='Forcer l\'arrêt sans confirmation')
@click.pass_context
@smart_resolve_params(session_id='session')
@trace_decorator
def kill_session(ctx, session_id, force):
    """🔴 Arrêter brutalement une session en cours"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    # Vérification si la session est active
    if session.get('status') != 'running':
        click.echo(f"⚠️ Session '{session['name']}' n'est pas en cours d'exécution")
        click.echo(f"   Status actuel: {session.get('status', 'unknown')}")
        return
    
    # Confirmation si pas de --force
    if not force:
        if not click.confirm(f"🔴 Confirmer l'arrêt brutal de la session '{session['name']}' ?"):
            click.echo("❌ Arrêt annulé")
            return
    
    # Arrêt de la session
    click.echo(f"🔴 Arrêt brutal de la session '{session['name']}'...")
    
    # Mise à jour du status
    session['status'] = 'killed'
    session['killed_at'] = datetime.now(timezone.utc).isoformat()
    
    # Arrêt des stratégies associées
    stopped_strategies = 0
    for strategy_id in session.get('strategies', []):
        if strategy_id in trading_cli.strategies_registry:
            strategy = trading_cli.strategies_registry[strategy_id]
            if strategy.get('status') == 'running':
                strategy['status'] = 'killed'
                strategy['killed_at'] = datetime.now(timezone.utc).isoformat()
                stopped_strategies += 1
    
    trading_cli._save_configuration()
    
    click.echo(f"✅ Session '{session['name']}' arrêtée brutalement")
    if stopped_strategies > 0:
        click.echo(f"🔴 {stopped_strategies} stratégie(s) également arrêtée(s)")

@session.command('remove')
@click.argument('session_id')
@click.option('--force', is_flag=True, help='Forcer la suppression sans confirmation')
@click.option('--keep-strategies', is_flag=True, help='Conserver les stratégies (ne supprimer que la session)')
@click.pass_context
@smart_resolve_params(session_id='session')
@trace_decorator
def remove_session(ctx, session_id, force, keep_strategies):
    """🗑️ Supprimer définitivement une session"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    # Vérification si la session est active
    if session.get('status') == 'running':
        click.echo(f"❌ Impossible de supprimer la session '{session['name']}' en cours d'exécution")
        click.echo("   Utilisez 'session kill' d'abord ou arrêtez-la proprement")
        return
    
    # Confirmation si pas de --force
    if not force:
        message = f"🗑️ Confirmer la suppression de la session '{session['name']}' ?"
        if session.get('strategies') and not keep_strategies:
            message += f"\n   ⚠️ Cela supprimera également {len(session['strategies'])} stratégie(s) associée(s)"
        
        if not click.confirm(message):
            click.echo("❌ Suppression annulée")
            return
    
    # Suppression des stratégies si demandé
    removed_strategies = 0
    if not keep_strategies and session.get('strategies'):
        for strategy_id in session.get('strategies', []):
            if strategy_id in trading_cli.strategies_registry:
                del trading_cli.strategies_registry[strategy_id]
                removed_strategies += 1
    
    # Suppression de la session
    session_name = session['name']
    del trading_cli.sessions_registry[session_id]
    
    trading_cli._save_configuration()
    
    click.echo(f"✅ Session '{session_name}' supprimée définitivement")
    if removed_strategies > 0:
        click.echo(f"🗑️ {removed_strategies} stratégie(s) également supprimée(s)")
    elif keep_strategies and session.get('strategies'):
        click.echo(f"💾 {len(session.get('strategies', []))} stratégie(s) conservée(s)")

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
@trace_decorator
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
@click.argument('session_queries', nargs=-1, required=True)
@click.option('--metric', default='performance', help='Métrique de comparaison')
@click.pass_context
@trace_decorator
def compare_sessions(ctx, session_queries, metric):
    """Comparaison avancée entre sessions (utilise la recherche intelligente)"""
    trading_cli = ctx.obj['cli']
    
    # Résoudre tous les IDs de sessions
    resolved_session_ids = []
    for session_query in session_queries:
        if SMART_FILTERS_AVAILABLE:
            session_id, error = UniversalFilter.resolve_session(session_query, trading_cli.sessions_registry)
            
            if error:
                click.echo(error)
                return
            
            # Afficher la résolution si différente
            if session_id != session_query:
                click.echo(f"🔍 Session résolue: '{session_query}' → '{session_id}'")
            
            resolved_session_ids.append(session_id)
        else:
            # Fallback sans smart filters
            if session_query not in trading_cli.sessions_registry:
                click.echo(f"❌ Session '{session_query}' introuvable!")
                return
            resolved_session_ids.append(session_query)
    
    click.echo(f"🔍 COMPARAISON DE {len(resolved_session_ids)} SESSIONS")
    click.echo("=" * 50)
    
    comparison_data = []
    
    for session_id in resolved_session_ids:
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
@trace_decorator
def list_strategies(ctx, filter_workflow, filter_active):
    """Lister les stratégies avec filtres"""
    trading_cli = ctx.obj['cli']
    
    # Initialiser le registre s'il n'existe pas
    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}
    
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
@click.option('--edit', is_flag=True, help='Lancer l\'édition directement après création')
@click.option('--no-edit', is_flag=True, help='Ne pas proposer d\'édition (par défaut propose)')
@click.pass_context
@trace_decorator
def create_strategy(ctx, name, strategy_id, no_workflow, workflow, interactive, edit, no_edit):
    """Créer une stratégie (standard ou workflow)"""
    trading_cli = ctx.obj['cli']
    
    # Déterminer si le workflow est activé (par défaut OUI, sauf si --no-workflow)
    workflow_enabled = not no_workflow  # Workflow par défaut, désactivé seulement avec --no-workflow
    
    # Support backward compatibility pour --workflow
    if workflow:
        workflow_enabled = True
        click.echo("⚠️ Option --workflow deprecated, workflow activé par défaut maintenant")
    
    # Initialiser le registre s'il n'existe pas
    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}
    
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
        'symbol': 'BTCUSDC',
        'timeframe': '15m',
        'indicators': {},
        'signal_rules': {
            'long_condition': None,
            'short_condition': None
        },
        'risk_profile': 'default',
        'risk_management': {},
        'start_date': '2025-01-01',
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
        timeframe = click.prompt('Timeframe', default='15m')
        strategy_config['timeframe'] = timeframe
        
        # Configuration de la balance initiale
        initial_balance = click.prompt('Balance initiale (€)', default=100.0, type=float)
        strategy_config['initial_balance'] = initial_balance
        strategy_config['current_balance'] = initial_balance  # Initialise à la même valeur

        # Configuration Risk Management
        # Risk management supprimé - maintenant géré au niveau des stratégies individuelles
    
    trading_cli.strategies_registry[strategy_id] = strategy_config
    trading_cli._save_configuration()
    
    click.echo(f"✅ Stratégie '{name}' créée avec l'ID '{strategy_id}'")
    if workflow_enabled:
        click.echo("🚀 Mode workflow avancé activé")
    else:
        click.echo("⚪ Mode standard (workflow désactivé)")
    
    # Logique d'édition automatique
    should_edit = False
    
    if edit:
        # Édition forcée avec --edit
        should_edit = True
    elif no_edit:
        # Pas d'édition avec --no-edit
        should_edit = False
    elif interactive:
        # Si mode interactif déjà utilisé, ne pas redemander
        click.echo("💡 Configuration interactive terminée")
        should_edit = False
    else:
        # Par défaut, proposer l'édition
        should_edit = click.confirm("\n📝 Voulez-vous configurer la stratégie maintenant?", default=True)
    
    if should_edit:
        click.echo("\n" + "="*60)
        click.echo("🎯 CONFIGURATION DE LA STRATÉGIE")
        click.echo("="*60)
        
        # Appeler directement la logique d'édition
        _edit_strategy_logic(ctx, strategy_id, 'all', trading_cli)

    else:
        click.echo("💡 Utilisez 'strategy edit' pour configurer les paramètres plus tard")

@strategy.command('show')
@click.argument('strategy_id')
@click.pass_context
@smart_resolve_params(strategy_id='strategy')
@trace_decorator
def show_strategy(ctx, strategy_id):
    """Afficher les détails complets d'une stratégie (utilise la recherche intelligente)"""
    trading_cli = ctx.obj['cli']
    
    # Initialiser le registre s'il n'existe pas
    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}
    
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
    
    # Balance de la stratégie
    initial_balance = strategy.get('initial_balance', 'N/A')
    current_balance = strategy.get('current_balance', initial_balance)
    if isinstance(current_balance, (int, float)) and isinstance(initial_balance, (int, float)):
        pnl = current_balance - initial_balance
        pnl_pct = (pnl / initial_balance * 100) if initial_balance > 0 else 0
        pnl_indicator = "📈" if pnl >= 0 else "📉"
        click.echo(f"  Balance initiale: {initial_balance}€")
        click.echo(f"  Balance courante: {current_balance}€")
        click.echo(f"  P&L: {pnl_indicator} {pnl:+.2f}€ ({pnl_pct:+.2f}%)")
    else:
        click.echo(f"  Balance initiale: {initial_balance}€")
        click.echo(f"  Balance courante: {current_balance}€")
    
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
    
    # Risk Management supprimé - maintenant géré au niveau des stratégies individuelles
    click.echo(f"  Risk Management:")
    for ind_name, ind_config in strategy.get('risk_management', {}).items():
        click.echo(f"  - {ind_name}: {ind_config}")

    click.echo(f"\n🔧 Indicateurs ({len(strategy.get('indicators', {}))}):")
    for ind_name, ind_config in strategy.get('indicators', {}).items():
        click.echo(f"  - {ind_name}: {ind_config.get('type', 'N/A')}({ind_config.get('parameters', {})})")
    
    # Performance de la stratégie
    performance = strategy.get('performance', {})
    if performance:
        click.echo(f"\n📊 PERFORMANCE DE LA STRATÉGIE:")
        
        total_trades = performance.get('total_trades', 0)
        winning_trades = performance.get('winning_trades', 0)
        losing_trades = performance.get('losing_trades', 0)
        win_rate = performance.get('win_rate', 0)
        
        click.echo(f"  🎯 Trades totaux: {total_trades}")
        click.echo(f"  🏆 Trades gagnants: {winning_trades}")
        click.echo(f"  💔 Trades perdants: {losing_trades}")
        
        win_rate_icon = "🎯" if win_rate >= 0.6 else "📈" if win_rate >= 0.4 else "📉"
        click.echo(f"  {win_rate_icon} Taux de réussite: {win_rate:.1%}")
        
        # Métriques avancées si disponibles
        if performance.get('profit_factor'):
            click.echo(f"  📊 Profit Factor: {performance.get('profit_factor', 0):.2f}")
        if performance.get('max_drawdown'):
            click.echo(f"  📉 Max Drawdown: {performance.get('max_drawdown', 0):.2f}%")
        if performance.get('sharpe_ratio'):
            click.echo(f"  🎯 Sharpe Ratio: {performance.get('sharpe_ratio', 0):.3f}")
            
        # Temps de trading
        if performance.get('start_time'):
            click.echo(f"  ⏰ Début: {performance.get('start_time', 'N/A')}")
        if performance.get('end_time'):
            click.echo(f"  🏁 Fin: {performance.get('end_time', 'N/A')}")
        elif strategy.get('status') == 'active':
            click.echo(f"  🟢 En cours d'exécution...")

def _show_stratey_logic(ctx, strategy, trading_cli):
    click.echo(f"📋 Détails de la stratégie :")
    click.echo(f"  Nom: {strategy['name']}")
    click.echo(f"  Paire: {strategy.get('symbol', 'Non défini')}")
    click.echo(f"  Timeframe: {strategy.get('timeframe', 'Non défini')}")
    click.echo(f"  Date début: {strategy.get('start_date', 'Non défini')}")
    click.echo(f"  Date fin: {strategy.get('end_date', 'now')}")

    click.echo(f"  Status: {strategy.get('status', 'inactive')}")
    click.echo(f"  Workflow: {'🚀 Activé' if strategy.get('workflow_enabled') else '❌ Désactivé'}")

    # Balance de la stratégie
    initial_balance = strategy.get('initial_balance', 'N/A')
    current_balance = strategy.get('current_balance', initial_balance)
    if isinstance(current_balance, (int, float)) and isinstance(initial_balance, (int, float)):
        pnl = current_balance - initial_balance
        pnl_pct = (pnl / initial_balance * 100) if initial_balance > 0 else 0
        pnl_indicator = "📈" if pnl >= 0 else "📉"
        click.echo(f"  Balance initiale: {initial_balance}€")
        click.echo(f"  Balance courante: {current_balance}€")
        click.echo(f"  P&L: {pnl_indicator} {pnl:+.2f}€ ({pnl_pct:+.2f}%)")
    else:
        click.echo(f"  Balance initiale: {initial_balance}€")
        click.echo(f"  Balance courante: {current_balance}€")

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

    # Risk Management supprimé - maintenant géré au niveau des stratégies individuelles

    click.echo(f"\n🔧 Indicateurs ({len(strategy.get('indicators', {}))}):")
    for ind_name, ind_config in strategy.get('indicators', {}).items():
        click.echo(f"  - {ind_name}: {ind_config.get('type', 'N/A')}({ind_config.get('parameters', {})})")

    click.echo(f"  Risk Management:")
    for ind_name, ind_config in strategy.get('risk_management', {}).items():
        click.echo(f"  - {ind_name}: {ind_config}")

def _edit_strategy_logic(ctx, strategy_id, section, trading_cli):
    """Logique d'édition de stratégie réutilisable"""
    
    # Initialiser le registre s'il n'existe pas
    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return False
    
    strategy = trading_cli.strategies_registry[strategy_id]
    original_strategy = strategy.copy()
    
    click.echo(f"✏️  MODIFICATION DE LA STRATÉGIE '{strategy_id}'")
    click.echo("=" * 60)
    click.echo("💡 Appuyez sur Entrée pour garder la valeur actuelle")
    click.echo("💡 Tapez 'skip' pour passer une section entière")
    click.echo()

    # Section: Paramètres de base
    if section in ['basic', 'all']:
        click.echo("📋 PARAMÈTRES DE BASE")
        click.echo("-" * 30)

        # Nom
        new_name = click.prompt(f"Nom [{strategy.get('name', 'Non défini')}]",
                                default=strategy.get('name', ''), show_default=False)
        if new_name and new_name != 'skip':
            strategy['name'] = new_name

        # Symbole
        new_symbol = click.prompt(f"Symbole/Paire [{strategy.get('symbol', 'Non défini')}]",
                                  default=strategy.get('symbol', ''), show_default=False)
        if new_symbol and new_symbol != 'skip':
            strategy['symbol'] = new_symbol

        # Timeframe
        new_timeframe = click.prompt(f"Timeframe [{strategy.get('timeframe', 'Non défini')}]",
                                     default=strategy.get('timeframe', ''), show_default=False)
        if new_timeframe and new_timeframe != 'skip':
            strategy['timeframe'] = new_timeframe

        # Start date
        new_start_date = click.prompt(f"Start date [{strategy.get('start_date', 'Non défini')}]",
                                      default=strategy.get('start_date', '2025-01-01'), show_default=False)
        if new_start_date and new_start_date != 'skip':
            strategy['start_date'] = new_start_date

        # End date
        new_end_date = click.prompt(f"End date [{strategy.get('end_date', 'Non défini')}]",
                                    default=strategy.get('end_date', 'now'), show_default=False)
        if new_end_date and new_end_date != 'skip':
            strategy['end_date'] = new_end_date

        # Balance initiale
        try:
            current_balance = strategy.get('initial_balance', 100.0)
            new_balance = click.prompt(f"Balance initiale [{current_balance}]",
                                       default=str(current_balance), show_default=False)
            if new_balance and new_balance != 'skip':
                strategy['initial_balance'] = float(new_balance)
        except ValueError:
            click.echo("⚠️ Balance invalide, valeur conservée")

        click.echo()

    # Section: Paramètres de stratégie
    if section in ['parameters', 'all']:
        click.echo("⚙️  PARAMÈTRES DE STRATÉGIE")
        click.echo("-" * 30)

        if not strategy.get('parameters'):
            strategy['parameters'] = {}

        # Afficher les paramètres existants et permettre modification
        for param_name, current_value in strategy.get('parameters', {}).items():
            new_value = click.prompt(f"{param_name} [{current_value}]",
                                     default=str(current_value), show_default=False)
            if new_value and new_value != 'skip':
                # Essayer de convertir au bon type
                try:
                    if isinstance(current_value, bool):
                        strategy['parameters'][param_name] = new_value.lower() in ['true', '1', 'yes', 'on']
                    elif isinstance(current_value, int):
                        strategy['parameters'][param_name] = int(new_value)
                    elif isinstance(current_value, float):
                        strategy['parameters'][param_name] = float(new_value)
                    else:
                        strategy['parameters'][param_name] = new_value
                except ValueError:
                    click.echo(f"⚠️ Valeur invalide pour {param_name}, conservée")

        # Proposer d'ajouter de nouveaux paramètres
        if click.confirm("Ajouter un nouveau paramètre?"):
            while True:
                param_name = click.prompt("Nom du paramètre (ou 'done' pour terminer)")
                if param_name.lower() == 'done':
                    break

                param_value = click.prompt(f"Valeur pour {param_name}")
                param_type = click.prompt("Type (str/int/float/bool)", default="str")

                try:
                    if param_type == 'int':
                        strategy['parameters'][param_name] = int(param_value)
                    elif param_type == 'float':
                        strategy['parameters'][param_name] = float(param_value)
                    elif param_type == 'bool':
                        strategy['parameters'][param_name] = param_value.lower() in ['true', '1', 'yes', 'on']
                    else:
                        strategy['parameters'][param_name] = param_value
                    click.echo(f"✅ Paramètre {param_name} ajouté")
                except ValueError:
                    click.echo(f"⚠️ Valeur invalide pour {param_name}")

        click.echo()

    # Section: Indicateurs
    if section in ['indicators', 'all']:
        click.echo("📈 INDICATEURS TECHNIQUES")
        click.echo("-" * 30)

        if not strategy.get('indicators'):
            strategy['indicators'] = {}

        # Modifier les indicateurs existants
        for ind_name, ind_config in strategy.get('indicators', {}).items():
            click.echo(f"\n🔹 Indicateur: {ind_name}")

            # Type d'indicateur
            current_type = ind_config.get('type', 'SMA')
            new_type = click.prompt(f"  Type [{current_type}]", default=current_type, show_default=False)
            if new_type and new_type != 'skip':
                ind_config['type'] = new_type

            # Paramètres de l'indicateur
            if not ind_config.get('parameters'):
                ind_config['parameters'] = {}

            for param_name, param_value in ind_config.get('parameters', {}).items():
                if param_name == 'name' and param_value is None:
                    param_value = ind_name
                new_param = click.prompt(f"  {param_name} [{param_value}]",
                                         default=str(param_value), show_default=False)
                if new_param and new_param != 'skip':
                    try:
                        if isinstance(param_value, int):
                            strategy['indicators'][ind_name]['parameters'][param_name] = int(new_param)

                        elif isinstance(param_value, float):
                            strategy['indicators'][ind_name]['parameters'][param_name] = float(new_param)
                        else:
                            strategy['indicators'][ind_name]['parameters'][param_name] = new_param
                    except ValueError:
                        click.echo(f"⚠️ Valeur invalide pour {param_name}")

        # Proposer d'ajouter de nouveaux indicateurs
        if click.confirm("Ajouter un nouvel indicateur?"):
            # Importer le service d'indicateurs pour les suggestions
            try:
                from services.indicators_service import IndicatorsService
                indicators_service = IndicatorsService()

                # Récupérer la liste des indicateurs disponibles (synchrone en utilisant le registre directement)
                from services.indicators_service import IndicatorRegistry
                available_indicators = IndicatorRegistry.list_indicators()
                available_types = list(available_indicators.keys()) if available_indicators else ['SMA', 'EMA', 'RSI',
                                                                                                  'MACD', 'BB', 'ATR',
                                                                                                  'STOCH']

                click.echo(f"\n📈 Indicateurs disponibles: {', '.join(available_types)}")

            except Exception as e:
                click.echo(f"⚠️ Impossible de charger les indicateurs: {e}")
                available_types = ['SMA', 'EMA', 'RSI', 'MACD', 'BB', 'ATR', 'STOCH']
                indicators_service = None

            while True:
                ind_name = click.prompt("Nom de l'indicateur (ou 'done' pour terminer)")
                if ind_name.lower() == 'done':
                    break

                # Proposer les types disponibles avec validation
                ind_type = click.prompt(f"Type d'indicateur {available_types}", default="SMA")

                # Vérifier si le type est valide
                if ind_type not in available_types:
                    click.echo(f"⚠️ Type '{ind_type}' non reconnu. Utilisation de 'SMA' par défaut")
                    ind_type = "SMA"

                strategy['indicators'][ind_name] = {
                    'type': ind_type,
                    'parameters': {},
                    'source': 'close'
                }

                # Utiliser les paramètres par défaut de l'indicateur si disponible
                if indicators_service:
                    try:
                        # Utiliser le registre directement (méthodes synchrones)
                        default_params = IndicatorRegistry.get_params(ind_type)
                        param_types = IndicatorRegistry.get_param_types(ind_type)

                        if default_params:
                            click.echo(f"\n🔧 Configuration automatique des paramètres pour {ind_type}:")
                            click.echo(f"Paramètres par défaut: {default_params}")

                            if click.confirm("Utiliser les paramètres par défaut?"):
                                strategy['indicators'][ind_name]['parameters'] = default_params.copy()
                                click.echo("✅ Paramètres par défaut appliqués")
                            else:
                                # Configuration manuelle avec suggestions
                                click.echo("Configuration manuelle des paramètres:")
                                for param_name, default_value in default_params.items():
                                    param_type_hint = param_types.get(param_name, type(
                                        default_value)).__name__ if param_types else 'auto'

                                    if param_name == 'name' and default_value is None:
                                        default_value = ind_name

                                    new_value = click.prompt(
                                        f"  {param_name} ({param_type_hint}) [{default_value}]",
                                        default=str(default_value),
                                        show_default=False
                                    )

                                    if new_value and new_value != str(default_value):
                                        # Convertir au bon type
                                        try:
                                            if param_types and param_types.get(param_name) == int:
                                                strategy['indicators'][ind_name]['parameters'][param_name] = int(
                                                    new_value)
                                            elif param_types and param_types.get(param_name) == float:
                                                strategy['indicators'][ind_name]['parameters'][param_name] = float(
                                                    new_value)
                                            elif param_types and param_types.get(param_name) == bool:
                                                strategy['indicators'][ind_name]['parameters'][
                                                    param_name] = new_value.lower() in ['true', '1', 'yes', 'on']
                                            elif isinstance(default_value, int):
                                                strategy['indicators'][ind_name]['parameters'][param_name] = int(
                                                    new_value)
                                            elif isinstance(default_value, float):
                                                strategy['indicators'][ind_name]['parameters'][param_name] = float(
                                                    new_value)
                                            elif isinstance(default_value, bool):
                                                strategy['indicators'][ind_name]['parameters'][
                                                    param_name] = new_value.lower() in ['true', '1', 'yes', 'on']
                                            else:
                                                strategy['indicators'][ind_name]['parameters'][param_name] = new_value
                                        except ValueError:
                                            click.echo(
                                                f"⚠️ Valeur invalide pour {param_name}, utilisation de la valeur par défaut")
                                            strategy['indicators'][ind_name]['parameters'][param_name] = default_value
                                    else:
                                        strategy['indicators'][ind_name]['parameters'][param_name] = default_value
                        else:
                            click.echo(f"⚠️ Aucun paramètre par défaut trouvé pour {ind_type}")
                            # Fallback vers la méthode manuelle
                            if click.confirm(f"Ajouter des paramètres pour {ind_name} manuellement?"):
                                while True:
                                    param_name = click.prompt("Nom du paramètre (ou 'done' pour terminer)")
                                    if param_name.lower() == 'done':
                                        break
                                    param_value = click.prompt(f"Valeur pour {param_name}")

                                    # Essayer de deviner le type
                                    try:
                                        if '.' in param_value:
                                            strategy['indicators'][ind_name]['parameters'][param_name] = float(
                                                param_value)
                                        else:
                                            strategy['indicators'][ind_name]['parameters'][param_name] = int(
                                                param_value)
                                    except ValueError:
                                        strategy['indicators'][ind_name]['parameters'][param_name] = param_value

                        # Proposer d'ajouter des paramètres supplémentaires
                        if click.confirm("Ajouter des paramètres supplémentaires?"):
                            while True:
                                extra_param = click.prompt("Nom du paramètre supplémentaire (ou 'done' pour terminer)")
                                if extra_param.lower() == 'done':
                                    break

                                extra_value = click.prompt(f"Valeur pour {extra_param}")
                                extra_type = click.prompt("Type (str/int/float/bool)", default="str")

                                try:
                                    if extra_type == 'int':
                                        strategy['indicators'][ind_name]['parameters'][extra_param] = int(extra_value)
                                    elif extra_type == 'float':
                                        strategy['indicators'][ind_name]['parameters'][extra_param] = float(extra_value)
                                    elif extra_type == 'bool':
                                        strategy['indicators'][ind_name]['parameters'][
                                            extra_param] = extra_value.lower() in ['true', '1', 'yes', 'on']
                                    else:
                                        strategy['indicators'][ind_name]['parameters'][extra_param] = extra_value
                                    click.echo(f"✅ Paramètre {extra_param} ajouté")
                                except ValueError:
                                    click.echo(f"⚠️ Valeur invalide pour {extra_param}")

                    except Exception as e:
                        click.echo(f"⚠️ Impossible de récupérer les paramètres par défaut: {e}")
                        # Fallback vers la méthode manuelle
                        if click.confirm(f"Ajouter des paramètres pour {ind_name} manuellement?"):
                            while True:
                                param_name = click.prompt("Nom du paramètre (ou 'done' pour terminer)")
                                if param_name.lower() == 'done':
                                    break
                                param_value = click.prompt(f"Valeur pour {param_name}")

                                # Essayer de deviner le type
                                try:
                                    if '.' in param_value:
                                        strategy['indicators'][ind_name]['parameters'][param_name] = float(param_value)
                                    else:
                                        strategy['indicators'][ind_name]['parameters'][param_name] = int(param_value)
                                except ValueError:
                                    strategy['indicators'][ind_name]['parameters'][param_name] = param_value
                else:
                    # Fallback si le service n'est pas disponible
                    if click.confirm(f"Ajouter des paramètres pour {ind_name}?"):
                        while True:
                            param_name = click.prompt("Nom du paramètre (ou 'done' pour terminer)")
                            if param_name.lower() == 'done':
                                break
                            param_value = click.prompt(f"Valeur pour {param_name}")

                            # Essayer de deviner le type
                            try:
                                if '.' in param_value:
                                    strategy['indicators'][ind_name]['parameters'][param_name] = float(param_value)
                                else:
                                    strategy['indicators'][ind_name]['parameters'][param_name] = int(param_value)
                            except ValueError:
                                strategy['indicators'][ind_name]['parameters'][param_name] = param_value

                click.echo(f"✅ Indicateur {ind_name} ({ind_type}) ajouté")

        click.echo()

    # Section: Règles de signaux
    if section in ['signals', 'all']:
        click.echo("🎯 RÈGLES DE SIGNAUX")
        click.echo("-" * 30)

        if not strategy.get('signal_rules'):
            strategy['signal_rules'] = {}

        # Condition LONG
        current_long = strategy.get('signal_rules', {}).get('long_condition', '')
        new_long = click.prompt(f"Condition LONG\n[{current_long}]\n➤ ",
                                default=current_long, show_default=False)
        if new_long and new_long != 'skip':
            strategy['signal_rules']['long_condition'] = new_long

        # Condition SHORT
        current_short = strategy.get('signal_rules', {}).get('short_condition', '')
        new_short = click.prompt(f"Condition SHORT\n[{current_short}]\n➤ ",
                                 default=current_short, show_default=False)
        if new_short and new_short != 'skip':
            strategy['signal_rules']['short_condition'] = new_short

        click.echo()


    # Section: Règles de signaux
    if section in ['risk_management', 'all']:
        click.echo("🎯 RÈGLES DE RISK MANAGEMENT")
        click.echo("-" * 30)

        if not strategy.get('risk_management'):
            strategy['risk_management'] = {}

        # list des profils de risk management

        click.echo(f"\n🔹 Risk profils: {trading_cli.risk_profils_registry.keys()}")
        # saisie du profil
        risk_profil = strategy['risk_management'].get('risk_profil', 'default')
        risk_profil = click.prompt(f"  Risk profil[{risk_profil}]", default=risk_profil, show_default=False)
        # saisie des parametres

        # Modifier les params du profil
        for risk_name, risk_value in trading_cli.risk_profils_registry.get(risk_profil, {}).items():

            new_risk = click.prompt(f"  {risk_name} [{risk_value}]",
                                     default=str(risk_value), show_default=False)
            if new_risk and risk_value != 'skip':
                try:
                    if isinstance(risk_value, int):
                        strategy['risk_management'][risk_name]= int(new_risk)

                    elif isinstance(risk_value, float):
                        strategy['risk_management'][risk_name]= float(new_risk)
                    else:
                        strategy['risk_management'][risk_name] = new_risk
                except ValueError:
                    click.echo(f"⚠️ Valeur invalide pour {risk_name}")

        click.echo()

    # Résumé des changements
    click.echo("📝 RÉSUMÉ DES MODIFICATIONS")
    click.echo("=" * 40)

    #changes_made = False
    #if strategy != original_strategy:
    #    changes_made = True
    click.echo("Stratégie modifiées:")
    _show_stratey_logic(ctx, strategy, trading_cli)

    # Confirmation et sauvegarde
    if click.confirm("\nSauvegarder les modifications?"):
        trading_cli.strategies_registry[strategy_id] = strategy
        trading_cli._save_configuration()
        click.echo("✅ Stratégie mise à jour avec succès!")

        # Si le workflow est activé, proposer de redémarrer
        if strategy.get('workflow_enabled') and strategy.get('status') == 'active':
            if click.confirm("La stratégie est active. Redémarrer pour appliquer les changements?"):
                # Logic pour redémarrer la stratégie
                click.echo("🔄 Redémarrage de la stratégie...")
    else:
        click.echo("❌ Modifications annulées")

    # Sauvegarder les modifications
    trading_cli.strategies_registry[strategy_id] = strategy
    trading_cli._save_configuration()

    click.echo()
    click.echo("✅ Configuration rapide terminée!")
    click.echo(f"💡 Pour une configuration avancée, utilisez: strategy edit {strategy_id}")

    return True

@strategy.command('edit')
@click.argument('strategy_id')
@click.option('--section', type=click.Choice(['basic', 'parameters', 'indicators', 'signals', 'all']), 
              default='all', help='Section à modifier')
@click.pass_context
@smart_resolve_params(strategy_id='strategy')
@trace_decorator
def edit_strategy(ctx, strategy_id, section):
    """Modifier une stratégie de manière interactive"""
    trading_cli = ctx.obj['cli']
    return _edit_strategy_logic(ctx, strategy_id, section, trading_cli)

@strategy.command('del')
@click.argument('strategy_id')
@click.option('--force', is_flag=True, help='Supprimer sans confirmation')
@click.option('--remove-from-sessions', is_flag=True, 
              help='Retirer aussi la stratégie de toutes les sessions')
@click.pass_context
@smart_resolve_params(strategy_id='strategy')
@trace_decorator
def delete_strategy(ctx, strategy_id, force, remove_from_sessions):
    """Supprimer une stratégie"""
    trading_cli = ctx.obj['cli']
    
    # Initialiser le registre s'il n'existe pas
    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    strategy = trading_cli.strategies_registry[strategy_id]
    
    # Vérifier si la stratégie est active
    if strategy.get('status') == 'active' or strategy.get('status') == 'running':
        click.echo(f"⚠️ La stratégie '{strategy['name']}' est actuellement active!")
        if not click.confirm("Voulez-vous vraiment la supprimer?"):
            click.echo("❌ Suppression annulée")
            return
    
    # Vérifier si la stratégie est utilisée dans des sessions
    sessions_using_strategy = []
    for session_id, session in trading_cli.sessions_registry.items():
        if strategy_id in session.get('strategies', []):
            sessions_using_strategy.append((session_id, session['name']))
    
    if sessions_using_strategy:
        click.echo(f"⚠️ La stratégie '{strategy['name']}' est utilisée dans {len(sessions_using_strategy)} session(s):")
        for session_id, session_name in sessions_using_strategy:
            click.echo(f"  • {session_id} ({session_name})")
        
        if remove_from_sessions:
            click.echo("🔧 La stratégie sera retirée de toutes ces sessions")
        else:
            click.echo("💡 Utilisez --remove-from-sessions pour la retirer automatiquement des sessions")
            if not click.confirm("Continuer la suppression sans retirer des sessions?"):
                click.echo("❌ Suppression annulée")
                return
    
    # Afficher les détails de la stratégie à supprimer
    click.echo(f"\n🗑️  SUPPRESSION DE LA STRATÉGIE")
    click.echo("=" * 50)
    click.echo(f"ID: {strategy_id}")
    click.echo(f"Nom: {strategy['name']}")
    click.echo(f"Type: {'🚀 Workflow' if strategy.get('workflow_enabled') else '⚪ Standard'}")
    click.echo(f"Status: {strategy.get('status', 'unknown')}")
    
    if strategy.get('performance'):
        perf = strategy['performance']
        click.echo(f"Performance: {perf.get('total_trades', 0)} trades, {perf.get('win_rate', 0):.1%} win rate")
    
    # Confirmation
    if not force:
        if not click.confirm(f"\n❗ Confirmer la suppression définitive de '{strategy['name']}'?"):
            click.echo("❌ Suppression annulée")
            return
    
    # Supprimer de toutes les sessions si demandé
    removed_from_sessions = 0
    if remove_from_sessions:
        for session_id, session_name in sessions_using_strategy:
            session = trading_cli.sessions_registry[session_id]
            if strategy_id in session.get('strategies', []):
                session['strategies'].remove(strategy_id)
                removed_from_sessions += 1
                click.echo(f"  ✅ Retirée de la session '{session_name}'")
    
    # Arrêter la stratégie si elle est active
    if strategy.get('status') in ['active', 'running']:
        strategy['status'] = 'stopped'
        click.echo("🛑 Stratégie arrêtée")
    
    # Supprimer la stratégie du registre
    del trading_cli.strategies_registry[strategy_id]
    
    # Sauvegarder la configuration
    trading_cli._save_configuration()
    
    # Résumé
    click.echo(f"\n✅ Stratégie '{strategy['name']}' supprimée avec succès!")
    if removed_from_sessions > 0:
        click.echo(f"📤 Retirée de {removed_from_sessions} session(s)")
    
    # Avertissement si encore dans des sessions
    remaining_sessions = len(sessions_using_strategy) - removed_from_sessions
    if remaining_sessions > 0:
        click.echo(f"⚠️ La stratégie reste référencée dans {remaining_sessions} session(s)")
        click.echo("💡 Utilisez 'session show <session_id>' pour vérifier")




#====================
# Grid generator
#===================
# À ajouter dans trading_cli_v4.py dans le groupe @strategy

@strategy.command('grid')
@click.argument('strategy_id')
@click.option('--name-pattern',
              help='Pattern pour nommer les stratégies (ex: {base_name}_EMA{indicators_LONG_parameters_period}_{indicators_SHORT_parameters_period})')
@click.option('--export', help='Exporter vers fichier YAML en plus du registre')
@click.option('--dry-run', is_flag=True, help='Simuler sans sauvegarder')
@click.option('--max-combinations', default=100, type=int, help='Limite du nombre de combinaisons (sécurité)')
@click.option('--confirm', is_flag=True, help='Demander confirmation avant génération')
@click.pass_context
@smart_resolve_params(strategy_id='strategy')
@trace_decorator
def grid_strategies(ctx, strategy_id, name_pattern, export, dry_run, max_combinations, confirm):
    """Générer un grid de stratégies à partir des valeurs séparées par '-'"""
    trading_cli = ctx.obj['cli']

    # Importer le générateur de grid
    from grid_generator import StrategyGridGenerator

    # Initialiser le registre s'il n'existe pas
    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}

    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return

    try:
        # Créer le générateur avec référence au CLI
        generator = StrategyGridGenerator(trading_cli)

        # Charger la stratégie de base depuis le registre
        base_strategy = generator.load_base_strategy_from_registry(strategy_id)
        base_strategy_name = base_strategy.get('name', strategy_id)

        click.echo(f"🎯 GÉNÉRATION GRID POUR: {base_strategy_name}")
        click.echo("=" * 60)

        # Analyser les paramètres de grid disponibles
        grid_params = generator.find_grid_parameters(base_strategy)

        if not grid_params:
            click.echo("❌ Aucun paramètre de grid détecté dans cette stratégie")
            click.echo("💡 Les paramètres de grid utilisent le format 'valeur_min-valeur_max'")
            click.echo("   Exemple: period: 20-30, stop_loss_percent: 0.5-4")
            return

        # Afficher les paramètres détectés
        click.echo("📊 PARAMÈTRES DE GRID DÉTECTÉS:")
        total_combinations = 1
        for param_path, values in grid_params.items():
            click.echo(f"  🔧 {param_path}: {values} ({len(values)} valeurs)")
            total_combinations *= len(values)

        click.echo(f"\n📈 TOTAL COMBINAISONS: {total_combinations}")

        # Vérification de sécurité
        if total_combinations > max_combinations:
            click.echo(f"⚠️ ATTENTION: {total_combinations} combinaisons dépasse la limite de {max_combinations}")
            click.echo(f"   Cela pourrait générer un très grand nombre de stratégies!")

            if not click.confirm("Continuer malgré tout?"):
                click.echo("❌ Génération annulée")
                return

        # Confirmation optionnelle
        if confirm and not dry_run:
            if not click.confirm(f"Générer {total_combinations} stratégies dans le registre?"):
                click.echo("❌ Génération annulée")
                return

        # Générer les combinaisons
        click.echo(f"\n🚀 GÉNÉRATION EN COURS...")
        strategies = generator.generate_strategy_combinations(
            base_strategy_name,
            name_pattern
        )

        if dry_run:
            # Mode simulation
            click.echo(f"\n🔍 MODE SIMULATION (DRY-RUN)")
            click.echo(f"✅ {len(strategies)} stratégies seraient générées")

            # Afficher quelques exemples
            click.echo(f"\n📋 EXEMPLES DE STRATÉGIES GÉNÉRÉES:")
            for i, strategy in enumerate(strategies[:5]):  # Montrer les 5 premières
                grid_info = strategy.get('grid_info', {})
                params = grid_info.get('parameters', {})
                click.echo(f"  {i + 1}. {strategy['id']}")
                click.echo(f"     Nom: {strategy['name']}")
                for param_path, value in params.items():
                    click.echo(f"     {param_path}: {value}")
                click.echo()

            if len(strategies) > 5:
                click.echo(f"  ... et {len(strategies) - 5} autres stratégies")

            click.echo("💡 Utilisez la commande sans --dry-run pour générer réellement")
            return

        # Sauvegarder dans le registre
        saved_count = generator.save_grid_strategies_to_cli(strategies)

        # Export optionnel vers fichier
        if export:
            generator.save_grid_strategies(strategies, export)
            click.echo(f"💾 Stratégies également exportées vers {export}")

        # Résumé
        click.echo(f"\n✅ GÉNÉRATION TERMINÉE!")
        click.echo(f"📊 {saved_count} nouvelles stratégies créées")
        click.echo(f"📁 Total stratégies dans le registre: {len(trading_cli.strategies_registry)}")

        # Afficher un aperçu des stratégies créées
        if saved_count > 0:
            click.echo(f"\n🎯 APERÇU DES STRATÉGIES CRÉÉES:")
            grid_strategies = [s for s in strategies if s['id'] in trading_cli.strategies_registry]

            for i, strategy in enumerate(grid_strategies[:3], 1):  # Montrer les 3 premières
                grid_info = strategy.get('grid_info', {})
                params = grid_info.get('parameters', {})
                click.echo(f"  {i}. {strategy['id']}")
                for param_path, value in params.items():
                    short_param = param_path.split('.')[-1]
                    click.echo(f"     {short_param}: {value}")

            if len(grid_strategies) > 3:
                click.echo(f"  ... et {len(grid_strategies) - 3} autres")

        # Suggestions d'utilisation
        click.echo(f"\n💡 PROCHAINES ÉTAPES:")
        click.echo(f"  • Utilisez 'strategy list --filter-workflow' pour voir toutes les stratégies")
        click.echo(f"  • Créez une session et ajoutez-y les stratégies grid")
        click.echo(f"  • Lancez 'performance best-strategies' pour comparer les résultats")

    except Exception as e:
        click.echo(f"❌ Erreur lors de la génération: {e}")
        if dry_run:
            click.echo("💡 Vérifiez la structure de votre stratégie de base")


# Et ajouter cette commande pour nettoyer les stratégies grid
@strategy.command('clean-grid')
@click.option('--base-strategy', help='Nettoyer seulement les stratégies d\'un base spécifique')
@click.option('--confirm', is_flag=True, help='Demander confirmation pour chaque suppression')
@click.option('--force', is_flag=True, help='Supprimer sans confirmation')
@click.pass_context
@trace_decorator
def clean_grid_strategies(ctx, base_strategy, confirm, force):
    """Nettoyer les stratégies générées par grid"""
    trading_cli = ctx.obj['cli']

    if trading_cli.strategies_registry is None:
        trading_cli.strategies_registry = {}

    # Trouver toutes les stratégies grid
    grid_strategies = []
    for strategy_id, strategy in trading_cli.strategies_registry.items():
        if strategy.get('grid_info'):
            if base_strategy and strategy['grid_info'].get('base_strategy') != base_strategy:
                continue
            grid_strategies.append((strategy_id, strategy))

    if not grid_strategies:
        filter_msg = f" pour '{base_strategy}'" if base_strategy else ""
        click.echo(f"❌ Aucune stratégie grid trouvée{filter_msg}")
        return

    click.echo(f"🧹 NETTOYAGE DES STRATÉGIES GRID")
    click.echo("=" * 50)
    click.echo(f"📊 {len(grid_strategies)} stratégies grid trouvées")

    # Grouper par stratégie de base
    by_base = {}
    for strategy_id, strategy in grid_strategies:
        base_name = strategy['grid_info'].get('base_strategy', 'Unknown')
        if base_name not in by_base:
            by_base[base_name] = []
        by_base[base_name].append((strategy_id, strategy))

    # Afficher le résumé
    for base_name, strategies in by_base.items():
        click.echo(f"  📁 {base_name}: {len(strategies)} stratégies")

    # Confirmation globale
    if not force:
        if not click.confirm(f"\n🗑️ Supprimer {len(grid_strategies)} stratégies grid?"):
            click.echo("❌ Nettoyage annulé")
            return

    # Suppression
    deleted_count = 0
    for strategy_id, strategy in grid_strategies:
        should_delete = True

        if confirm and not force:
            base_name = strategy['grid_info'].get('base_strategy', 'Unknown')
            should_delete = click.confirm(f"Supprimer '{strategy_id}' (base: {base_name})?")

        if should_delete:
            del trading_cli.strategies_registry[strategy_id]
            deleted_count += 1

    # Sauvegarder
    trading_cli._save_configuration()

    click.echo(f"\n✅ {deleted_count} stratégies grid supprimées")
    click.echo(f"📁 {len(trading_cli.strategies_registry)} stratégies restantes dans le registre")



# ============================================================================
# COMMANDES PERFORMANCE TRACKING
# ============================================================================

@cli.group()
def performance():
    """📊 Gestion des performances et rankings"""
    pass

@performance.command('best-strategies')
@click.option('--session-id', help='Filtrer par session spécifique')
@click.option('--limit', default=10, type=int, help='Nombre de stratégies à afficher')
@click.option('--metric', type=click.Choice(['pnl', 'win_rate', 'profit_factor']), 
              default='pnl', help='Métrique de tri')
@click.pass_context
@trace_decorator
def best_strategies(ctx, session_id, limit, metric):
    """Afficher les meilleures stratégies par session ou globalement"""
    trading_cli = ctx.obj['cli']
    
    strategies_performance = []
    
    # Filtrer par session si spécifiée
    if session_id:
        if session_id not in trading_cli.sessions_registry:
            click.echo(f"❌ Session '{session_id}' introuvable!")
            return
        
        session = trading_cli.sessions_registry[session_id]
        strategy_ids = session.get('strategies', [])
        click.echo(f"📊 Meilleures stratégies de la session '{session['name']}'")
    else:
        strategy_ids = list(trading_cli.strategies_registry.keys())
        click.echo(f"📊 Meilleures stratégies (toutes sessions)")
    
    # Collecter les performances
    for strategy_id in strategy_ids:
        if strategy_id in trading_cli.strategies_registry:
            strategy = trading_cli.strategies_registry[strategy_id]
            performance = strategy.get('performance', {})
            
            if not performance:
                continue
                
            pnl = 0
            if strategy.get('current_balance') and strategy.get('initial_balance'):
                pnl = strategy['current_balance'] - strategy['initial_balance']
            
            strategies_performance.append({
                'id': strategy_id,
                'name': strategy['name'],
                'session': session_id or _find_strategy_session(trading_cli, strategy_id),
                'pnl': pnl,
                'win_rate': performance.get('win_rate', 0),
                'profit_factor': performance.get('profit_factor', 0),
                'total_trades': performance.get('total_trades', 0),
                'balance': strategy.get('current_balance', 0)
            })
    
    if not strategies_performance:
        click.echo("❌ Aucune donnée de performance trouvée")
        return
    
    # Trier selon la métrique
    strategies_performance.sort(key=lambda x: x[metric], reverse=True)
    strategies_performance = strategies_performance[:limit]
    
    click.echo("=" * 80)
    table_data = []
    
    for i, strategy in enumerate(strategies_performance, 1):
        rank_icon = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}"
        pnl_icon = "📈" if strategy['pnl'] >= 0 else "📉"
        
        table_data.append([
            rank_icon,
            strategy['name'][:20] + "..." if len(strategy['name']) > 20 else strategy['name'],
            strategy['session'][:12] + "..." if len(strategy['session']) > 12 else strategy['session'],
            f"{pnl_icon} {strategy['pnl']:.2f}€",
            f"{strategy['win_rate']:.1%}",
            f"{strategy['profit_factor']:.2f}",
            strategy['total_trades']
        ])
    
    headers = ['Rank', 'Stratégie', 'Session', 'P&L', 'Win Rate', 'Profit Factor', 'Trades']
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # Champion absolu
    if strategies_performance:
        champion = strategies_performance[0]
        click.echo(f"\n🏆 CHAMPION: {champion['name']}")
        click.echo(f"  💰 P&L: {champion['pnl']:.2f}€")
        click.echo(f"  🎯 Win Rate: {champion['win_rate']:.1%}")
        click.echo(f"  📊 Profit Factor: {champion['profit_factor']:.2f}")

@performance.command('profitable-sessions')
@click.option('--limit', default=10, type=int, help='Nombre de sessions à afficher')
@click.option('--metric', type=click.Choice(['total_pnl', 'avg_strategy_pnl', 'best_strategy_pnl']), 
              default='total_pnl', help='Métrique de tri')
@click.pass_context
@trace_decorator
def profitable_sessions(ctx, limit, metric):
    """Lister les sessions les plus profitables"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.sessions_registry:
        click.echo("❌ Aucune session trouvée")
        return
    
    session_performances = []
    
    for session_id, session in trading_cli.sessions_registry.items():
        strategies = session.get('strategies', [])
        
        if not strategies:
            continue
            
        total_pnl = 0
        strategy_pnls = []
        valid_strategies = 0
        
        # Calculer P&L total et par stratégie
        for strategy_id in strategies:
            if strategy_id in trading_cli.strategies_registry:
                strategy = trading_cli.strategies_registry[strategy_id]
                
                if strategy.get('current_balance') and strategy.get('initial_balance'):
                    strategy_pnl = strategy['current_balance'] - strategy['initial_balance']
                    total_pnl += strategy_pnl
                    strategy_pnls.append(strategy_pnl)
                    valid_strategies += 1
        
        if valid_strategies == 0:
            continue
            
        avg_strategy_pnl = total_pnl / valid_strategies if valid_strategies > 0 else 0
        best_strategy_pnl = max(strategy_pnls) if strategy_pnls else 0
        
        session_performances.append({
            'id': session_id,
            'name': session['name'],
            'mode': session['mode'],
            'total_pnl': total_pnl,
            'avg_strategy_pnl': avg_strategy_pnl,
            'best_strategy_pnl': best_strategy_pnl,
            'strategies_count': valid_strategies,
            'created_at': session.get('created_at', '')
        })
    
    if not session_performances:
        click.echo("❌ Aucune donnée de performance trouvée")
        return
    
    # Trier selon la métrique
    session_performances.sort(key=lambda x: x[metric], reverse=True)
    session_performances = session_performances[:limit]
    
    click.echo(f"📊 TOP {limit} SESSIONS LES PLUS PROFITABLES (par {metric})")
    click.echo("=" * 90)
    
    table_data = []
    for i, session in enumerate(session_performances, 1):
        rank_icon = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}"
        pnl_icon = "📈" if session['total_pnl'] >= 0 else "📉"
        
        table_data.append([
            rank_icon,
            session['name'][:25] + "..." if len(session['name']) > 25 else session['name'],
            session['mode'],
            session['strategies_count'],
            f"{pnl_icon} {session['total_pnl']:.2f}€",
            f"{session['avg_strategy_pnl']:.2f}€",
            f"{session['best_strategy_pnl']:.2f}€",
            session['created_at'][:10]
        ])
    
    headers = ['Rank', 'Session', 'Mode', 'Stratégies', 'P&L Total', 'P&L Moyen', 'Meilleur P&L', 'Créé']
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # Session championne
    if session_performances:
        champion = session_performances[0]
        click.echo(f"\n🏆 SESSION CHAMPIONNE: {champion['name']}")
        click.echo(f"  💰 P&L Total: {champion['total_pnl']:.2f}€")
        click.echo(f"  📊 {champion['strategies_count']} stratégies")
        click.echo(f"  🎯 P&L moyen par stratégie: {champion['avg_strategy_pnl']:.2f}€")
        click.echo(f"  🌟 Meilleure stratégie: {champion['best_strategy_pnl']:.2f}€")

def _find_strategy_session(trading_cli, strategy_id):
    """Trouve la session d'une stratégie"""
    for session_id, session in trading_cli.sessions_registry.items():
        if strategy_id in session.get('strategies', []):
            return session_id
    return "N/A"

def _update_strategy_performance_on_completion(trading_cli, strategy_id):
    """Met à jour les performances d'une stratégie à la fin de son exécution"""
    if strategy_id not in trading_cli.strategies_registry:
        return
        
    strategy = trading_cli.strategies_registry[strategy_id]
    
    # Simuler des données de performance (dans un vrai système, on récupérerait depuis la DB)
    import random
    
    performance = {
        'total_trades': random.randint(10, 100),
        'winning_trades': 0,
        'losing_trades': 0,
        'win_rate': 0,
        'profit_factor': 0,
        'max_drawdown': random.uniform(0, 15),
        'sharpe_ratio': random.uniform(-1, 3),
        'end_time': datetime.now(timezone.utc).isoformat()
    }
    
    # Calculer win rate et autres métriques
    total_trades = performance['total_trades']
    performance['winning_trades'] = random.randint(int(total_trades * 0.3), int(total_trades * 0.8))
    performance['losing_trades'] = total_trades - performance['winning_trades']
    performance['win_rate'] = performance['winning_trades'] / total_trades if total_trades > 0 else 0
    performance['profit_factor'] = random.uniform(0.5, 3.0)
    
    strategy['performance'] = performance
    trading_cli._save_configuration()
    
    return performance

def _update_session_best_strategy(trading_cli, session_id):
    """Met à jour la meilleure stratégie d'une session"""
    if session_id not in trading_cli.sessions_registry:
        return
        
    session = trading_cli.sessions_registry[session_id]
    strategies = session.get('strategies', [])
    
    if not strategies:
        return
        
    best_strategy = None
    best_pnl = float('-inf')
    
    for strategy_id in strategies:
        if strategy_id in trading_cli.strategies_registry:
            strategy = trading_cli.strategies_registry[strategy_id]
            
            if strategy.get('current_balance') and strategy.get('initial_balance'):
                pnl = strategy['current_balance'] - strategy['initial_balance']
                
                if pnl > best_pnl:
                    best_pnl = pnl
                    best_strategy = {
                        'id': strategy_id,
                        'name': strategy['name'],
                        'pnl': pnl,
                        'performance': strategy.get('performance', {})
                    }
    
    if best_strategy:
        session['best_strategy'] = best_strategy
        session['session_completed_at'] = datetime.now(timezone.utc).isoformat()
        trading_cli._save_configuration()
        
        return best_strategy
    
    return None

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
@trace_decorator
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
                pairs=enhanced_config.pairs,
                since=enhanced_config.since,
                to=enhanced_config.to
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
@trace_decorator
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
# GESTION DES INDICATEURS TECHNIQUES
# ============================================================================

@cli.group()
def indicators():
    """📈 Gestion des indicateurs techniques"""
    pass

@indicators.command('list')
@click.option('--category', help='Filtrer par catégorie (moving_averages, oscillators, momentum, volatility)')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), 
              default='table', help='Format de sortie')
@click.pass_context
@trace_decorator
def indicators_list(ctx, category, output_format):
    """Lister tous les indicateurs techniques disponibles"""
    
    async def _list_indicators():
        try:
            # Importer le service d'indicateurs
            from services.indicators_service import IndicatorsService
            
            indicators_service = IndicatorsService()
            indicators_df = await indicators_service.list_available_indicators()
            
            if indicators_df.empty:
                click.echo("❌ Aucun indicateur disponible")
                return
            
            # Filtrer par catégorie si spécifié
            if category:
                indicators_df = indicators_df[indicators_df['Category'] == category]
                if indicators_df.empty:
                    click.echo(f"❌ Aucun indicateur trouvé pour la catégorie '{category}'")
                    return
            
            if output_format == 'json':
                click.echo(indicators_df.to_json(orient='records', indent=2))
            else:
                click.echo("\n📈 INDICATEURS TECHNIQUES DISPONIBLES")
                click.echo("=" * 80)
                
                if category:
                    click.echo(f"Catégorie: {category}")
                    click.echo("-" * 40)
                
                # Grouper par catégorie pour un affichage plus clair
                if not category:
                    for cat in indicators_df['Category'].unique():
                        cat_indicators = indicators_df[indicators_df['Category'] == cat]
                        click.echo(f"\n🏷️  {cat.upper()}")
                        click.echo("-" * 40)
                        
                        for _, indicator in cat_indicators.iterrows():
                            click.echo(f"  📊 {indicator['Name']}")
                            click.echo(f"     Description: {indicator['Description']}")
                            click.echo(f"     Paramètres: {indicator['Default_Params']}")
                            click.echo(f"     Colonnes requises: {indicator['Required_Columns']}")
                            click.echo(f"     Colonnes de sortie: {indicator['Output_Columns']}")
                            click.echo()
                else:
                    for _, indicator in indicators_df.iterrows():
                        click.echo(f"📊 {indicator['Name']}")
                        click.echo(f"   Description: {indicator['Description']}")
                        click.echo(f"   Paramètres: {indicator['Default_Params']}")
                        click.echo(f"   Colonnes requises: {indicator['Required_Columns']}")
                        click.echo(f"   Colonnes de sortie: {indicator['Output_Columns']}")
                        click.echo()
                
                click.echo(f"\n📊 Total: {len(indicators_df)} indicateur(s)")
                
                if not category:
                    categories = indicators_df['Category'].unique()
                    click.echo(f"🏷️  Catégories disponibles: {', '.join(categories)}")
                    click.echo("\n💡 Utilisez --category <nom> pour filtrer par catégorie")
                
        except Exception as e:
            click.echo(f"❌ Erreur lors de la récupération des indicateurs: {e}")
    
    asyncio.run(_list_indicators())

# ============================================================================
# COMMANDES UTILITAIRES
# ============================================================================

@cli.command('entities')
@click.option('--type', 'entity_type', type=click.Choice(['sessions', 'strategies', 'all']), 
              default='all', help='Type d\'entités à lister')
@click.pass_context
@trace_decorator
def list_entities(ctx, entity_type):
    """Lister toutes les entités disponibles pour les filtres intelligents"""
    trading_cli = ctx.obj['cli']
    
    if entity_type in ['sessions', 'all']:
        click.echo("📅 SESSIONS DISPONIBLES:")
        if not trading_cli.sessions_registry:
            click.echo("  Aucune session configurée")
        else:
            for session_id, session in trading_cli.sessions_registry.items():
                click.echo(f"  • {session_id} ({session['name']})")
    
    if entity_type in ['strategies', 'all']:
        click.echo("\n🎯 STRATÉGIES DISPONIBLES:")
        if not trading_cli.strategies_registry:
            click.echo("  Aucune stratégie configurée")
        else:
            for strategy_id, strategy in trading_cli.strategies_registry.items():
                click.echo(f"  • {strategy_id} ({strategy['name']})")
    
    if entity_type == 'all':
        click.echo("\n💡 UTILISATION DES FILTRES:")
        click.echo("  Les commandes acceptent maintenant les noms partiels:")
        click.echo("  - IDs complets: demo_unified_session")
        click.echo("  - Noms partiels: demo, unified, etc.")
        click.echo("  - Recherche floue automatique avec suggestions")

@cli.command('version')
@trace_decorator
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
@trace_decorator
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
                'start_date': '2025-01-01',
                'end_date': 'now',
                'parameters': {},
                'indicators': {
                    'SMA_10': {'type': 'SMA', 'parameters': {'period': 10}, 'source': 'close'},
                    'SMA_30': {'type': 'SMA', 'parameters': {'period': 30}, 'source': 'close'},
                    'RSI_14': {'type': 'RSI', 'parameters': {'period': 14}, 'source': 'close'}
                },
                'signal_rules': {
                    'long_condition': 'SMA_10 > SMA_30 & SMA_10[-1] <= SMA_30[-1] & RSI_14 < 70',
                    'short_condition': 'SMA_10 < SMA_30 & SMA_10[-1] >= SMA_30[-1] & RSI_14 > 30'
                },
                'risk_management': {
                    'profil': 'default',
                    'position_size_value': 2.0,
                    'max_concurrent_trades': 3,
                    'stop_loss_percent': 2.0,
                    'take_profit_percent': 4.0,
                    'risk_ratio': 1.5
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
                'mode': 'paper',
                'workflow_enabled': True,
                'strategies': ['demo_unified'],
                'status': 'configured',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        },
        'risk_profils': {
            'default': {
                'name': 'Profil par défaut',
                'position_size_value': 2.0,
                'max_concurrent_trades': 3,
                'stop_loss_percent': 2.0,
                'take_profit_percent': 4.0,
                'RR': 1.5
            }
        }
    }
    
    # Sauvegarder
    trading_cli.strategies_registry = default_config['strategies']
    trading_cli.sessions_registry = default_config['sessions']
    trading_cli.risk_profils_registry = default_config['risk_profils']
    trading_cli._save_configuration()
    
    click.echo("✅ Système initialisé avec configuration par défaut")
    click.echo("💡 Utilisez 'session list' pour voir les sessions disponibles")
    click.echo("🚀 Utilisez 'test workflow' pour tester le système")

if __name__ == '__main__':
    cli()