"""
TradingCLI V3 - CLI Avancé avec Workflow Stratégique Intégré
Fusion du système de queue/interpréteur avec l'interface CLI complète
"""

import click
import asyncio
import json
import yaml
from typing import List, Dict, Optional
from datetime import datetime, timezone
# from tabulate import tabulate  # Optional for better formatting
try:
    from tabulate import tabulate
except ImportError:
    def tabulate(data, headers=None, tablefmt='grid'):
        """Simple fallback for tabulate"""
        if not data:
            return "No data"
        
        # Simple text table
        result = []
        if headers:
            result.append(" | ".join(str(h) for h in headers))
            result.append("-" * (len(" | ".join(str(h) for h in headers))))
        
        for row in data:
            result.append(" | ".join(str(cell) for cell in row))
        
        return "\n".join(result)
import os
import sys

# Imports pour le workflow avancé
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.strategy_workflow import TradingStrategy, IndicatorConfig, SignalRule
from core.indicators_service import IndicatorsService
from core.strategy_manager_enhanced import StrategyManagerEnhanced, EnhancedStrategyConfig
from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from data.models import StrategyConfig, SessionMode
from data.persistence import DatabaseManager

class TradingCLI:
    def __init__(self, config_path: str = "config/trading_config.yaml"):
        self.config_path = config_path
        
        # Composants du workflow avancé
        self.db_manager = None
        self.pubsub = None
        self.strategy_manager = None
        
        # Registres
        self.strategies_registry = {}
        self.indicators_registry = {}
        self.risk_profils_registry = {}
        self.sessions_registry = {}
        
        # État du système
        self.is_running = False
        
        # Charger configuration
        self._load_configuration()
    
    def _load_configuration(self):
        """Charge la configuration depuis les fichiers"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                self.strategies_registry = config.get('strategies', {})
                self.indicators_registry = config.get('indicators', {})
                self.risk_profils_registry = config.get('risk_profils', {})
                self.sessions_registry = config.get('sessions', {})
    
    def _save_configuration(self):
        """Sauvegarde la configuration"""
        config = {
            'strategies': self.strategies_registry,
            'indicators': self.indicators_registry,
            'risk_profils': self.risk_profils_registry,
            'sessions': self.sessions_registry
        }
        
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)
    
    async def initialize_system(self):
        """Initialise le système de workflow avancé"""
        if self.is_running:
            return
        
        try:
            # Initialiser les composants
            self.db_manager = DatabaseManager()
            self.pubsub = PubSubEngine()
            
            await self.pubsub.start()
            
            self.strategy_manager = StrategyManagerEnhanced(self.db_manager, self.pubsub)
            await self.strategy_manager.initialize()
            
            self.is_running = True
            click.echo("✅ Système de workflow avancé initialisé")
            
        except Exception as e:
            click.echo(f"❌ Erreur d'initialisation: {e}")
            raise
    
    async def shutdown_system(self):
        """Arrête le système"""
        if not self.is_running:
            return
        
        try:
            if self.strategy_manager:
                await self.strategy_manager.shutdown()
            
            if self.pubsub:
                await self.pubsub.stop()
            
            self.is_running = False
            click.echo("✅ Système arrêté proprement")
            
        except Exception as e:
            click.echo(f"❌ Erreur lors de l'arrêt: {e}")
    
    def convert_to_enhanced_config(self, strategy_data: dict) -> EnhancedStrategyConfig:
        """Convertit une stratégie du registre vers EnhancedStrategyConfig"""
        base_config = StrategyConfig(
            name=strategy_data['name'],
            parameters=strategy_data.get('parameters', {}),
            timeframe=strategy_data.get('timeframe', '5m'),
            pairs=[strategy_data.get('symbol', 'BTCUSD')]
        )
        
        enhanced = EnhancedStrategyConfig(base_config)
        
        # Appliquer les indicateurs personnalisés s'ils existent
        if 'indicators' in strategy_data:
            enhanced.indicators = {}
            for ind_name, ind_config in strategy_data['indicators'].items():
                enhanced.indicators[ind_name] = IndicatorConfig(
                    type=ind_config['type'],
                    parameters=ind_config['parameters'],
                    source=ind_config.get('source', 'close')
                )
        
        # Appliquer les règles de signaux personnalisées
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

@click.group()
@click.pass_context
def cli(ctx):
    """Trading CLI V3 - Configurateur Avancé avec Workflow Stratégique"""
    ctx.ensure_object(dict)
    ctx.obj['cli'] = TradingCLI()

# ============================================================================
# COMMANDES SYSTÈME
# ============================================================================

@cli.group()
def system():
    """Gestion du système de trading avancé"""
    pass

@system.command('start')
@click.pass_context
def start_system(ctx):
    """Démarrer le système de workflow avancé"""
    trading_cli = ctx.obj['cli']
    
    async def _start():
        await trading_cli.initialize_system()
    
    asyncio.run(_start())

@system.command('stop')
@click.pass_context
def stop_system(ctx):
    """Arrêter le système"""
    trading_cli = ctx.obj['cli']
    
    async def _stop():
        await trading_cli.shutdown_system()
    
    asyncio.run(_stop())

@system.command('status')
@click.pass_context
def system_status(ctx):
    """Afficher le statut du système"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.is_running:
        click.echo("❌ Système arrêté")
        click.echo("💡 Utilisez 'system start' pour démarrer le système")
        return
    
    async def _status():
        # Obtenir les métriques globales
        global_metrics = trading_cli.strategy_manager.get_global_metrics()
        active_strategies = trading_cli.strategy_manager.get_active_strategies()
        
        click.echo("🟢 Système de workflow avancé actif")
        click.echo(f"📊 Stratégies actives: {len(active_strategies)}")
        click.echo(f"📈 Total candles traitées: {global_metrics.get('total_candles_processed', 0)}")
        click.echo(f"🎯 Total signaux générés: {global_metrics.get('total_signals_generated', 0)}")
        click.echo(f"⏱️ Uptime: {global_metrics.get('uptime_seconds', 0):.1f} secondes")
        
        # Métriques des indicateurs
        indicators_metrics = global_metrics.get('indicators_service_metrics', {})
        click.echo(f"🔧 Indicateurs calculés: {indicators_metrics.get('calculations_performed', 0)}")
        click.echo(f"⚡ Cache hit rate: {indicators_metrics.get('cache_hit_rate', 0):.1f}%")
        
        if active_strategies:
            click.echo(f"\n📋 Stratégies actives:")
            for strategy_id in active_strategies:
                status = await trading_cli.strategy_manager.get_strategy_status(strategy_id)
                if status:
                    click.echo(f"  - {status['name']}: {status['dataframe_info'].get('rows', 0)} candles, queue={status['queue_size']}")
    
    if trading_cli.is_running:
        asyncio.run(_status())

@system.command('metrics')
@click.pass_context
def system_metrics(ctx):
    """Afficher les métriques détaillées du système"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.is_running:
        click.echo("❌ Système arrêté")
        return
    
    async def _metrics():
        global_metrics = trading_cli.strategy_manager.get_global_metrics()
        
        click.echo("📊 MÉTRIQUES SYSTÈME DÉTAILLÉES")
        click.echo("=" * 50)
        
        # Métriques globales
        click.echo("🌐 GLOBALES:")
        for key, value in global_metrics.items():
            if key != 'indicators_service_metrics':
                click.echo(f"  {key}: {value}")
        
        # Métriques des indicateurs
        indicators_metrics = global_metrics.get('indicators_service_metrics', {})
        click.echo("\n🔧 SERVICE D'INDICATEURS:")
        for key, value in indicators_metrics.items():
            click.echo(f"  {key}: {value}")
        
        # Métriques par stratégie active
        active_strategies = trading_cli.strategy_manager.get_active_strategies()
        if active_strategies:
            click.echo(f"\n📈 STRATÉGIES ACTIVES ({len(active_strategies)}):")
            for strategy_id in active_strategies:
                status = await trading_cli.strategy_manager.get_strategy_status(strategy_id)
                if status:
                    click.echo(f"\n  {strategy_id} ({status['name']}):")
                    click.echo(f"    Queue: {status['queue_size']}")
                    click.echo(f"    DataFrame: {status['dataframe_info'].get('rows', 0)} rows")
                    click.echo(f"    Métriques: {status['metrics']}")
    
    asyncio.run(_metrics())

# ============================================================================
# COMMANDES STRATÉGIES AVANCÉES
# ============================================================================

@cli.group()
def strategy():
    """Gestion des stratégies de trading avancées"""
    pass

@strategy.command('list')
@click.pass_context
def list_strategies(ctx):
    """Lister toutes les stratégies existantes"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.strategies_registry:
        click.echo("Aucune stratégie configurée.")
        return
    
    # Préparer les données pour le tableau
    table_data = []
    for strategy_id, strategy_config in trading_cli.strategies_registry.items():
        # Statut avancé si le système est running
        advanced_status = "N/A"
        if trading_cli.is_running:
            active_strategies = trading_cli.strategy_manager.get_active_strategies()
            if strategy_id in active_strategies:
                advanced_status = "🟢 ACTIVE"
            else:
                advanced_status = "⚪ CONFIGURED"
        
        table_data.append([
            strategy_id,
            strategy_config.get('name', 'N/A'),
            strategy_config.get('symbol', 'N/A'),
            strategy_config.get('timeframe', 'N/A'),
            len(strategy_config.get('indicators', {})),
            strategy_config.get('risk_profile', 'default'),
            advanced_status
        ])
    
    headers = ['ID', 'Nom', 'Paire', 'Timeframe', 'Indicateurs', 'Risk Profile', 'Status']
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))

@strategy.command('create')
@click.option('--name', prompt='Nom de la stratégie', help='Nom de la stratégie')
@click.option('--id', 'strategy_id', prompt='ID unique', help='Identifiant unique')
@click.option('--workflow', is_flag=True, help='Configurer pour le workflow avancé')
@click.pass_context
def create_strategy(ctx, name, strategy_id, workflow):
    """Créer une nouvelle stratégie (compatible workflow avancé)"""
    trading_cli = ctx.obj['cli']
    
    if strategy_id in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' existe déjà!")
        return
    
    # Configuration de base
    strategy_config = {
        'id': strategy_id,
        'name': name,
        'symbol': None,
        'timeframe': None,
        'parameters': {},  # Pour EnhancedStrategyConfig
        'indicators': {},
        'signal_rules': {
            'long_condition': None,
            'short_condition': None
        },
        'risk_profile': 'default',
        'start_date': None,
        'end_date': 'now',
        'status': 'inactive',
        'workflow_enabled': workflow,
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    
    if workflow:
        click.echo("🚀 Mode workflow avancé activé")
        click.echo("💡 Cette stratégie utilisera le système de queue et l'interpréteur de signaux")
    
    trading_cli.strategies_registry[strategy_id] = strategy_config
    trading_cli._save_configuration()
    
    click.echo(f"✅ Stratégie '{name}' créée avec l'ID '{strategy_id}'")
    click.echo("💡 Utilisez 'strategy configure' pour configurer les paramètres")

@strategy.command('configure')
@click.argument('strategy_id')
@click.pass_context
def configure_strategy(ctx, strategy_id):
    """Configurer une stratégie existante de manière interactive"""
    trading_cli = ctx.obj['cli']
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    strategy = trading_cli.strategies_registry[strategy_id]
    click.echo(f"📝 Configuration de la stratégie '{strategy['name']}'")
    
    if strategy.get('workflow_enabled'):
        click.echo("🚀 Mode workflow avancé détecté")
    
    # Configuration de la paire
    current_symbol = strategy.get('symbol', 'Non défini')
    new_symbol = click.prompt(f'Paire de trading (actuel: {current_symbol})', 
                             default=strategy.get('symbol', ''), 
                             show_default=False)
    if new_symbol:
        strategy['symbol'] = new_symbol
    
    # Configuration du timeframe
    current_timeframe = strategy.get('timeframe', 'Non défini')
    timeframe_choices = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d']
    click.echo(f"Timeframes disponibles: {', '.join(timeframe_choices)}")
    new_timeframe = click.prompt(f'Timeframe (actuel: {current_timeframe})',
                                default=strategy.get('timeframe', ''),
                                show_default=False)
    if new_timeframe and new_timeframe in timeframe_choices:
        strategy['timeframe'] = new_timeframe
    
    # Configuration des paramètres pour EnhancedStrategyConfig
    if strategy.get('workflow_enabled'):
        click.echo("\n🔧 Configuration des paramètres pour le workflow:")
        
        # Configuration des fenêtres pour SMA
        if 'short_window' not in strategy.get('parameters', {}):
            short_window = click.prompt('Fenêtre courte pour SMA', default=10, type=int)
            long_window = click.prompt('Fenêtre longue pour SMA', default=30, type=int)
            
            if 'parameters' not in strategy:
                strategy['parameters'] = {}
            strategy['parameters']['short_window'] = short_window
            strategy['parameters']['long_window'] = long_window
    
    # Configuration des dates
    current_start = strategy.get('start_date', 'Non défini')
    new_start_date = click.prompt(f'Date de début (YYYY-MM-DD) (actuel: {current_start})',
                                 default=strategy.get('start_date', ''),
                                 show_default=False)
    if new_start_date:
        try:
            datetime.fromisoformat(new_start_date)
            strategy['start_date'] = new_start_date
        except ValueError:
            click.echo("⚠️ Format de date invalide, ignoré")
    
    current_end = strategy.get('end_date', 'now')
    new_end_date = click.prompt(f'Date de fin (YYYY-MM-DD ou "now") (actuel: {current_end})',
                               default=strategy.get('end_date', 'now'),
                               show_default=False)
    if new_end_date:
        strategy['end_date'] = new_end_date
    
    # Configuration des signaux (compatible avec l'interpréteur)
    if strategy.get('workflow_enabled'):
        click.echo("\n📈 Configuration des signaux (syntaxe interpréteur):")
        click.echo("Exemples de conditions avancées:")
        click.echo("  - SMA_10 > SMA_30 & SMA_10[-1] <= SMA_30[-1] & RSI_14 < 70")
        click.echo("  - close > SMA_20 & EMA_50 > EMA_50[-1] & RSI_14 > 50")
    
    current_long = strategy['signal_rules'].get('long_condition', 'Non défini')
    click.echo("📈 Configuration du signal LONG")
    new_long_condition = click.prompt(f'Expression signal LONG (actuel: {current_long})',
                                     default=strategy['signal_rules'].get('long_condition', ''),
                                     show_default=False)
    if new_long_condition:
        strategy['signal_rules']['long_condition'] = new_long_condition
    
    current_short = strategy['signal_rules'].get('short_condition', 'Non défini')
    click.echo("📉 Configuration du signal SHORT")
    new_short_condition = click.prompt(f'Expression signal SHORT (actuel: {current_short})',
                                      default=strategy['signal_rules'].get('short_condition', ''),
                                      show_default=False)
    if new_short_condition:
        strategy['signal_rules']['short_condition'] = new_short_condition
    
    trading_cli._save_configuration()
    click.echo(f"✅ Stratégie '{strategy_id}' configurée avec succès!")

@strategy.command('start')
@click.argument('strategy_id')
@click.pass_context
def start_strategy(ctx, strategy_id):
    """Démarrer une stratégie dans le workflow avancé"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.is_running:
        click.echo("❌ Système arrêté. Utilisez 'system start' d'abord")
        return
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    strategy_data = trading_cli.strategies_registry[strategy_id]
    
    async def _start_strategy():
        try:
            # Convertir vers EnhancedStrategyConfig
            enhanced_config = trading_cli.convert_to_enhanced_config(strategy_data)
            
            # Créer StrategyConfig de base
            base_config = StrategyConfig(
                name=enhanced_config.name,
                parameters=enhanced_config.parameters,
                timeframe=enhanced_config.timeframe,
                pairs=enhanced_config.pairs
            )
            
            # Démarrer avec le strategy manager
            success = await trading_cli.strategy_manager.start_strategy(
                strategy_id, 
                base_config
            )
            
            if success:
                strategy_data['status'] = 'active'
                trading_cli._save_configuration()
                click.echo(f"🚀 Stratégie '{strategy_data['name']}' démarrée dans le workflow avancé")
                
                # Afficher les indicateurs configurés
                status = await trading_cli.strategy_manager.get_strategy_status(strategy_id)
                if status:
                    df_info = status['dataframe_info']
                    click.echo(f"📊 DataFrame: {df_info.get('rows', 0)} rows, {len(df_info.get('columns', []))} columns")
                    if df_info.get('columns'):
                        click.echo(f"📈 Colonnes: {', '.join(df_info.get('columns', []))}")
            else:
                click.echo(f"❌ Échec du démarrage de la stratégie '{strategy_id}'")
        
        except Exception as e:
            click.echo(f"❌ Erreur lors du démarrage: {e}")
    
    asyncio.run(_start_strategy())

@strategy.command('stop')
@click.argument('strategy_id')
@click.pass_context
def stop_strategy(ctx, strategy_id):
    """Arrêter une stratégie"""
    trading_cli = ctx.obj['cli']
    
    if not trading_cli.is_running:
        click.echo("❌ Système arrêté")
        return
    
    async def _stop_strategy():
        success = await trading_cli.strategy_manager.stop_strategy(strategy_id)
        if success:
            if strategy_id in trading_cli.strategies_registry:
                trading_cli.strategies_registry[strategy_id]['status'] = 'stopped'
                trading_cli._save_configuration()
            click.echo(f"⏹️ Stratégie '{strategy_id}' arrêtée")
        else:
            click.echo(f"❌ Échec de l'arrêt de la stratégie '{strategy_id}'")
    
    asyncio.run(_stop_strategy())

@strategy.command('status')
@click.argument('strategy_id')
@click.pass_context
def strategy_status(ctx, strategy_id):
    """Afficher le statut détaillé d'une stratégie"""
    trading_cli = ctx.obj['cli']
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    strategy_data = trading_cli.strategies_registry[strategy_id]
    
    click.echo(f"📋 Statut de la stratégie '{strategy_id}':")
    click.echo(f"  Nom: {strategy_data['name']}")
    click.echo(f"  Workflow avancé: {'🟢 Oui' if strategy_data.get('workflow_enabled') else '❌ Non'}")
    click.echo(f"  Statut local: {strategy_data.get('status', 'unknown')}")
    
    if trading_cli.is_running:
        async def _get_advanced_status():
            status = await trading_cli.strategy_manager.get_strategy_status(strategy_id)
            if status:
                click.echo(f"\n🚀 STATUT WORKFLOW AVANCÉ:")
                click.echo(f"  Running: {'🟢 Oui' if status['is_running'] else '❌ Non'}")
                click.echo(f"  Processing: {'🟡 Oui' if status['processing_candle'] else '⚪ Non'}")
                click.echo(f"  Queue size: {status['queue_size']}")
                
                df_info = status['dataframe_info']
                click.echo(f"\n📊 DATAFRAME:")
                click.echo(f"  Rows: {df_info.get('rows', 0)}")
                click.echo(f"  Columns: {len(df_info.get('columns', []))}")
                if df_info.get('columns'):
                    click.echo(f"  Available: {', '.join(df_info.get('columns', []))}")
                if df_info.get('last_timestamp'):
                    click.echo(f"  Last update: {df_info.get('last_timestamp')}")
                
                metrics = status['metrics']
                click.echo(f"\n📈 MÉTRIQUES:")
                click.echo(f"  Candles processed: {metrics['candles_processed']}")
                click.echo(f"  Signals generated: {metrics['signals_generated']}")
                click.echo(f"  Indicators calculated: {metrics['indicators_calculated']}")
                click.echo(f"  Avg processing time: {metrics['avg_processing_time']:.3f}s")
                click.echo(f"  Errors: {metrics['errors']}")
                
                # Performance si disponible
                performance = await trading_cli.strategy_manager.get_strategy_performance(strategy_id)
                if performance:
                    click.echo(f"\n💰 PERFORMANCE:")
                    click.echo(f"  Current balance: {performance['current_capital']:.2f}€")
                    click.echo(f"  Total P&L: {performance['total_pnl']:.2f}€ ({performance['total_pnl_pct']:.2f}%)")
                    click.echo(f"  Win rate: {performance['win_rate']:.1f}%")
            else:
                click.echo("⚪ Stratégie non active dans le workflow")
        
        asyncio.run(_get_advanced_status())
    else:
        click.echo("⚪ Système arrêté - statut avancé indisponible")

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
    click.echo(f"  Workflow avancé: {'🟢 Activé' if strategy.get('workflow_enabled') else '❌ Désactivé'}")
    
    # Paramètres du workflow
    if strategy.get('parameters'):
        click.echo(f"\n⚙️ Paramètres:")
        for param, value in strategy['parameters'].items():
            click.echo(f"  {param}: {value}")
    
    click.echo("\n📈 Signaux:")
    click.echo(f"  LONG: {strategy['signal_rules'].get('long_condition', 'Non défini')}")
    click.echo(f"  SHORT: {strategy['signal_rules'].get('short_condition', 'Non défini')}")
    
    click.echo(f"\n🔧 Indicateurs ({len(strategy.get('indicators', {}))}):")
    for ind_name, ind_config in strategy.get('indicators', {}).items():
        click.echo(f"  - {ind_name}: {ind_config}")
    
    # Si le workflow est activé, simuler la configuration EnhancedStrategyConfig
    if strategy.get('workflow_enabled'):
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
# COMMANDES INDICATEURS (reprendre les existantes)
# ============================================================================

@cli.group()
def indicator():
    """Gestion des indicateurs techniques"""
    pass

@indicator.command('list')
@click.pass_context
def list_indicators(ctx):
    """Lister tous les indicateurs disponibles"""
    # Indicateurs supportés par le workflow avancé
    advanced_indicators = {
        'RSI': {
            'type': 'RSI',
            'description': 'Relative Strength Index',
            'parameters': {'period': 14},
            'source': 'close',
            'workflow_support': '🟢'
        },
        'SMA': {
            'type': 'SMA', 
            'description': 'Simple Moving Average',
            'parameters': {'period': 20},
            'source': 'close',
            'workflow_support': '🟢'
        },
        'EMA': {
            'type': 'EMA',
            'description': 'Exponential Moving Average', 
            'parameters': {'period': 20},
            'source': 'close',
            'workflow_support': '🟢'
        },
        'MACD': {
            'type': 'MACD',
            'description': 'Moving Average Convergence Divergence',
            'parameters': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
            'source': 'close',
            'workflow_support': '🟢'
        },
        'BB': {
            'type': 'BollingerBands',
            'description': 'Bollinger Bands',
            'parameters': {'period': 20, 'std_dev': 2},
            'source': 'close',
            'workflow_support': '🟢'
        },
        'ATR': {
            'type': 'ATR',
            'description': 'Average True Range',
            'parameters': {'period': 14},
            'source': 'hlc',
            'workflow_support': '🟢'
        },
        'STOCH': {
            'type': 'Stochastic',
            'description': 'Stochastic Oscillator',
            'parameters': {'k_period': 14, 'd_period': 3, 'smooth_k': 3},
            'source': 'hlc',
            'workflow_support': '🟢'
        }
    }
    
    table_data = []
    for name, config in advanced_indicators.items():
        params_str = ', '.join([f"{k}={v}" for k, v in config['parameters'].items()])
        table_data.append([
            name,
            config['description'],
            params_str,
            config['source'],
            config['workflow_support']
        ])
    
    headers = ['Nom', 'Description', 'Paramètres par défaut', 'Source', 'Workflow']
    click.echo("🔧 Indicateurs disponibles (avec support workflow avancé):")
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))

@indicator.command('add')
@click.argument('strategy_id')
@click.argument('indicator_name')
@click.pass_context
def add_indicator_to_strategy(ctx, strategy_id, indicator_name):
    """Ajouter un indicateur à une stratégie"""
    trading_cli = ctx.obj['cli']
    
    if strategy_id not in trading_cli.strategies_registry:
        click.echo(f"❌ Stratégie '{strategy_id}' introuvable!")
        return
    
    strategy = trading_cli.strategies_registry[strategy_id]
    
    # Vérifier si l'indicateur existe déjà
    existing_indicators = strategy.get('indicators', {})
    
    # Proposer un nom unique
    base_name = indicator_name.upper()
    unique_name = base_name
    counter = 1
    
    while unique_name in existing_indicators:
        unique_name = f"{base_name}_{counter}"
        counter += 1
    
    # Configuration interactive de l'indicateur
    click.echo(f"🔧 Configuration de l'indicateur {indicator_name}")
    
    if strategy.get('workflow_enabled'):
        click.echo("🚀 Mode workflow avancé détecté - Configuration optimisée")
    
    if indicator_name.upper() == 'RSI':
        period = click.prompt('Période', default=14, type=int)
        
        indicator_config = {
            'type': 'RSI',
            'parameters': {'period': period},
            'source': 'close'
        }
    
    elif indicator_name.upper() in ['SMA', 'EMA']:
        period = click.prompt('Période', default=20, type=int)
        
        indicator_config = {
            'type': indicator_name.upper(),
            'parameters': {'period': period},
            'source': 'close'
        }
    
    elif indicator_name.upper() == 'MACD':
        fast = click.prompt('Période rapide', default=12, type=int)
        slow = click.prompt('Période lente', default=26, type=int)
        signal = click.prompt('Période signal', default=9, type=int)
        
        indicator_config = {
            'type': 'MACD',
            'parameters': {
                'fast_period': fast,
                'slow_period': slow,
                'signal_period': signal
            },
            'source': 'close'
        }
    
    elif indicator_name.upper() == 'BB':
        period = click.prompt('Période', default=20, type=int)
        std_dev = click.prompt('Écart-type', default=2.0, type=float)
        
        indicator_config = {
            'type': 'BollingerBands',
            'parameters': {
                'period': period,
                'std_dev': std_dev
            },
            'source': 'close'
        }
    
    else:
        click.echo(f"❌ Indicateur '{indicator_name}' non supporté!")
        click.echo("Indicateurs supportés: RSI, SMA, EMA, MACD, BB, ATR, STOCH")
        return
    
    # Demander le nom final
    final_name = click.prompt('Nom de l\'indicateur dans la stratégie', default=unique_name)
    
    # Ajouter à la stratégie
    if 'indicators' not in strategy:
        strategy['indicators'] = {}
    
    strategy['indicators'][final_name] = indicator_config
    trading_cli._save_configuration()
    
    click.echo(f"✅ Indicateur '{final_name}' ajouté à la stratégie '{strategy_id}'")
    
    if strategy.get('workflow_enabled'):
        click.echo("🚀 Cet indicateur sera automatiquement calculé par le workflow avancé")

# ============================================================================
# COMMANDES SESSIONS AVANCÉES
# ============================================================================

@cli.group()
def session():
    """Gestion des sessions de trading avancées"""
    pass

@session.command('create')
@click.option('--name', prompt='Nom de la session', help='Nom de la session')
@click.option('--mode', 
              type=click.Choice(['sandbox', 'paper', 'live']),
              prompt='Mode de trading',
              help='Mode de trading')
@click.option('--workflow', is_flag=True, help='Session pour workflow avancé')
@click.pass_context
def create_session(ctx, name, mode, workflow):
    """Créer une nouvelle session de trading"""
    trading_cli = ctx.obj['cli']
    
    session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    session_config = {
        'id': session_id,
        'name': name,
        'mode': mode,
        'workflow_enabled': workflow,
        'strategies': [],
        'status': 'created',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'start_time': None,
        'end_time': None
    }
    
    trading_cli.sessions_registry[session_id] = session_config
    trading_cli._save_configuration()
    
    click.echo(f"✅ Session '{name}' créée avec l'ID '{session_id}'")
    if workflow:
        click.echo("🚀 Session configurée pour le workflow avancé")
    click.echo(f"💡 Utilisez 'session add-strategy {session_id} <strategy_id>' pour ajouter des stratégies")

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
    
    # Si c'est une session workflow, s'assurer que le système est démarré
    if session.get('workflow_enabled') and not trading_cli.is_running:
        click.echo("🚀 Session workflow détectée - démarrage du système...")
        
        async def _init_and_start():
            await trading_cli.initialize_system()
            await _start_session_strategies()
        
        async def _start_session_strategies():
            success_count = 0
            for strategy_id in session.get('strategies', []):
                if strategy_id in trading_cli.strategies_registry:
                    strategy_data = trading_cli.strategies_registry[strategy_id]
                    
                    # Convertir et démarrer
                    enhanced_config = trading_cli.convert_to_enhanced_config(strategy_data)
                    base_config = StrategyConfig(
                        name=enhanced_config.name,
                        parameters=enhanced_config.parameters,
                        timeframe=enhanced_config.timeframe,
                        pairs=enhanced_config.pairs
                    )
                    
                    success = await trading_cli.strategy_manager.start_strategy(strategy_id, base_config)
                    if success:
                        success_count += 1
                        strategy_data['status'] = 'active'
            
            session['status'] = 'running'
            session['start_time'] = datetime.now(timezone.utc).isoformat()
            trading_cli._save_configuration()
            
            click.echo(f"🚀 Session '{session['name']}' démarrée avec {success_count} stratégies actives")
        
        asyncio.run(_init_and_start())
    
    else:
        # Session normale (pas de workflow)
        # Validation normale
        missing_config = []
        for strategy_id in session.get('strategies', []):
            if strategy_id not in trading_cli.strategies_registry:
                missing_config.append(f"Stratégie '{strategy_id}' introuvable")
                continue
        
        if missing_config:
            click.echo("❌ Configuration incomplète:")
            for error in missing_config:
                click.echo(f"  - {error}")
            return
        
        session['status'] = 'running'
        session['start_time'] = datetime.now(timezone.utc).isoformat()
        
        for strategy_id in session.get('strategies', []):
            trading_cli.strategies_registry[strategy_id]['status'] = 'active'
        
        trading_cli._save_configuration()
        click.echo(f"🚀 Session '{session['name']}' démarrée")

@session.command('show')
@click.argument('session_id')
@click.pass_context
def show_session(ctx, session_id):
    """Afficher les détails d'une session avec performances"""
    trading_cli = ctx.obj['cli']
    
    if session_id not in trading_cli.sessions_registry:
        click.echo(f"❌ Session '{session_id}' introuvable!")
        return
    
    session = trading_cli.sessions_registry[session_id]
    
    click.echo(f"📋 Détails de la session '{session_id}':")
    click.echo(f"  Nom: {session['name']}")
    click.echo(f"  Mode: {session['mode']}")
    click.echo(f"  Status: {session.get('status', 'unknown')}")
    click.echo(f"  Workflow avancé: {'🟢 Oui' if session.get('workflow_enabled') else '❌ Non'}")
    click.echo(f"  Créé le: {session.get('created_at', 'N/A')}")
    
    if session.get('start_time'):
        click.echo(f"  Démarré le: {session['start_time']}")
    
    click.echo(f"\n🎯 Stratégies ({len(session.get('strategies', []))}):")
    for strategy_id in session.get('strategies', []):
        if strategy_id in trading_cli.strategies_registry:
            strategy = trading_cli.strategies_registry[strategy_id]
            status_icon = "🟢" if strategy.get('status') == 'active' else "⚪"
            click.echo(f"  {status_icon} {strategy_id}: {strategy['name']}")
            
            # Performance en temps réel si workflow actif
            if trading_cli.is_running and session.get('workflow_enabled'):
                async def _get_realtime_perf():
                    perf = await trading_cli.strategy_manager.get_strategy_performance(strategy_id)
                    if perf:
                        click.echo(f"    💰 Balance: {perf['current_capital']:.2f}€ (P&L: {perf['total_pnl']:.2f}€)")
                        click.echo(f"    📊 Trades: {perf['total_trades']}, Signaux: {perf['signals_generated']}")
                
                asyncio.run(_get_realtime_perf())
        else:
            click.echo(f"  ❌ {strategy_id}: Stratégie introuvable")

# ============================================================================
# COMMANDES TEST ET EXEMPLES
# ============================================================================

@cli.group()
def test():
    """Commandes de test pour le workflow avancé"""
    pass

@test.command('workflow')
@click.option('--duration', default=30, type=int, help='Durée du test en secondes')
@click.pass_context
def test_workflow(ctx, duration):
    """Tester le workflow avancé avec données simulées"""
    trading_cli = ctx.obj['cli']
    
    async def _run_test():
        click.echo("🧪 Démarrage du test du workflow avancé...")
        
        # Initialiser le système
        await trading_cli.initialize_system()
        
        # Créer une stratégie de test
        test_strategy_config = {
            'name': 'TestWorkflowStrategy',
            'symbol': 'BTCUSD',
            'timeframe': '5m',
            'parameters': {'short_window': 3, 'long_window': 5},
            'indicators': {},
            'signal_rules': {
                'long_condition': 'SMA_3 > SMA_5 & SMA_3[-1] <= SMA_5[-1] & RSI_14 < 70',
                'short_condition': 'SMA_3 < SMA_5 & SMA_3[-1] >= SMA_5[-1] & RSI_14 > 30'
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
        
        # Démarrer la stratégie
        test_session_id = "test_workflow_session"
        success = await trading_cli.strategy_manager.start_strategy(test_session_id, base_config)
        
        if not success:
            click.echo("❌ Échec du démarrage de la stratégie de test")
            return
        
        click.echo("✅ Stratégie de test démarrée")
        
        # Simuler des données de marché
        from examples.strategy_workflow_example import MockMarketDataGenerator
        
        market_generator = MockMarketDataGenerator(trading_cli.pubsub)
        generation_task = asyncio.create_task(market_generator.start_generating(interval=1.0))
        
        click.echo(f"📈 Génération de données de marché pendant {duration} secondes...")
        
        # Monitoring en temps réel
        start_time = asyncio.get_event_loop().time()
        last_status_time = 0
        
        while asyncio.get_event_loop().time() - start_time < duration:
            current_time = asyncio.get_event_loop().time()
            
            # Afficher le statut toutes les 5 secondes
            if current_time - last_status_time >= 5:
                status = await trading_cli.strategy_manager.get_strategy_status(test_session_id)
                if status:
                    df_info = status['dataframe_info']
                    metrics = status['metrics']
                    
                    elapsed = current_time - start_time
                    click.echo(f"⏱️ {elapsed:.0f}s - Candles: {df_info.get('rows', 0)}, "
                              f"Indicateurs: {metrics['indicators_calculated']}, "
                              f"Signaux: {metrics['signals_generated']}")
                
                last_status_time = current_time
            
            await asyncio.sleep(1)
        
        # Arrêter la génération
        market_generator.stop_generating()
        generation_task.cancel()
        
        # Résultats finaux
        final_status = await trading_cli.strategy_manager.get_strategy_status(test_session_id)
        if final_status:
            df_info = final_status['dataframe_info']
            metrics = final_status['metrics']
            
            click.echo(f"\n🎉 Test terminé!")
            click.echo(f"📊 Résultats finaux:")
            click.echo(f"  Candles traitées: {metrics['candles_processed']}")
            click.echo(f"  Indicateurs calculés: {metrics['indicators_calculated']}")
            click.echo(f"  Signaux générés: {metrics['signals_generated']}")
            click.echo(f"  Temps de traitement moyen: {metrics['avg_processing_time']:.3f}s")
            click.echo(f"  Erreurs: {metrics['errors']}")
            click.echo(f"  DataFrame final: {df_info.get('rows', 0)} rows, {len(df_info.get('columns', []))} columns")
            
            if df_info.get('columns'):
                click.echo(f"  Colonnes disponibles: {', '.join(df_info.get('columns', []))}")
        
        # Arrêter la stratégie
        await trading_cli.strategy_manager.stop_strategy(test_session_id)
        
        # Arrêter le système
        await trading_cli.shutdown_system()
        
        click.echo("✅ Test du workflow terminé avec succès!")
    
    asyncio.run(_run_test())

@test.command('example')
@click.pass_context
def test_example(ctx):
    """Lancer l'exemple de workflow intégré"""
    click.echo("🧪 Lancement de l'exemple de workflow intégré...")
    
    # Importer et lancer l'exemple existant
    try:
        from examples.strategy_workflow_example import run_tests
        asyncio.run(run_tests())
    except ImportError:
        click.echo("❌ Module d'exemple non trouvé")
    except Exception as e:
        click.echo(f"❌ Erreur lors de l'exécution de l'exemple: {e}")

# ============================================================================
# COMMANDES PRINCIPALES
# ============================================================================

@cli.command('version')
def version():
    """Afficher la version"""
    click.echo("TradingCLI V3 - Version avec Workflow Stratégique Avancé")
    click.echo("Features:")
    click.echo("  🚀 Workflow de traitement séquentiel avec queue")
    click.echo("  🔧 Calculs parallèles d'indicateurs avec synchronisation")
    click.echo("  📊 Interpréteur de conditions avancé pour signaux")
    click.echo("  📈 DataFrame intégré avec métriques temps réel")
    click.echo("  🎯 Communication PubSub asynchrone")
    click.echo("  💰 Gestion complète des sessions et performances")

if __name__ == '__main__':
    cli()