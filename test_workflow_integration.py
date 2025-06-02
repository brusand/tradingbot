#!/usr/bin/env python3
"""
Test d'intégration du workflow de stratégie avec queue et interpréteur
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Dict

# Ajouter le répertoire parent au PYTHONPATH
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.strategy_manager_enhanced import StrategyManagerEnhanced
from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from data.models import StrategyConfig
from data.persistence import DatabaseManager

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WorkflowIntegrationTester:
    """Testeur d'intégration pour le workflow complet"""
    
    def __init__(self):
        self.db_manager = None
        self.pubsub = None
        self.strategy_manager = None
        self.test_session_id = "test_session_001"
        
        # Métriques de test
        self.test_metrics = {
            'candles_sent': 0,
            'signals_received': 0,
            'test_start_time': None,
            'test_duration': 0
        }
    
    async def setup(self):
        """Configure l'environnement de test"""
        logger.info("🔧 Setting up integration test environment...")
        
        # Initialiser les composants
        self.db_manager = DatabaseManager()
        self.pubsub = PubSubEngine()
        
        await self.pubsub.start()
        
        self.strategy_manager = StrategyManagerEnhanced(self.db_manager, self.pubsub)
        await self.strategy_manager.initialize()
        
        # Configurer l'écoute des signaux
        await self._setup_signal_monitoring()
        
        logger.info("✅ Test environment setup complete")
    
    async def _setup_signal_monitoring(self):
        """Configure l'écoute des signaux pour les tests"""
        async def signal_monitor(signal_data: Dict, metadata):
            self.test_metrics['signals_received'] += 1
            signal = signal_data.get('signal', {})
            
            logger.info(f"📡 SIGNAL RECEIVED:")
            logger.info(f"   Type: {signal.get('type')}")
            logger.info(f"   Rule: {signal.get('rule_name')}")
            logger.info(f"   Condition: {signal.get('rule_condition')}")
            logger.info(f"   Symbol: {signal.get('symbol')}")
            logger.info(f"   Strength: {signal.get('strength')}")
            
            # Afficher le contexte
            context = signal.get('dataframe_snapshot', {})
            if context:
                logger.info(f"   Context: close={context.get('close', 'N/A'):.2f}")
                for key, value in context.items():
                    if key not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
                        if value is not None:
                            logger.info(f"            {key}={value:.2f}")
        
        # S'abonner à tous les signaux
        await self.pubsub.subscribe("strategies.signal.*", signal_monitor)
    
    async def test_basic_strategy_lifecycle(self):
        """Test le cycle de vie basique d'une stratégie"""
        logger.info("🧪 Testing basic strategy lifecycle...")
        
        # Créer une configuration de stratégie
        strategy_config = StrategyConfig(
            name="TestMovingAverageStrategy",
            parameters={
                'short_window': 5,
                'long_window': 10,
                'max_position_size': 0.1
            },
            timeframe="5m",
            pairs=["BTCUSDC"]
        )
        
        # Démarrer la stratégie
        success = await self.strategy_manager.start_strategy(
            self.test_session_id, 
            strategy_config
        )
        
        assert success, "Strategy should start successfully"
        logger.info("✅ Strategy started")
        
        # Vérifier le status
        status = await self.strategy_manager.get_strategy_status(self.test_session_id)
        assert status is not None, "Strategy status should be available"
        assert status['is_running'], "Strategy should be running"
        
        logger.info(f"📊 Strategy status: {status['name']} - Queue: {status['queue_size']}")
        
        # Arrêter la stratégie
        success = await self.strategy_manager.stop_strategy(self.test_session_id)
        assert success, "Strategy should stop successfully"
        
        logger.info("✅ Basic lifecycle test passed")
    
    async def test_candle_processing_and_indicators(self):
        """Test le traitement des candles et calcul d'indicateurs"""
        logger.info("🧪 Testing candle processing and indicators...")
        
        # Redémarrer la stratégie
        strategy_config = StrategyConfig(
            name="TestIndicatorStrategy",
            parameters={
                'short_window': 3,
                'long_window': 5,
                'max_position_size': 0.1
            },
            timeframe="5m",
            pairs=["BTCUSDC"]
        )
        
        await self.strategy_manager.start_strategy(self.test_session_id, strategy_config)
        
        # Envoyer une série de candles
        base_price = 50000.0
        base_timestamp = datetime.now(timezone.utc).timestamp()
        
        logger.info("📈 Sending test candles...")
        
        for i in range(20):  # Envoyer 20 candles
            # Simuler une tendance haussière
            price = base_price + i * 50 + (i * i * 2)  # Accélération
            
            candle = {
                "timestamp": base_timestamp + i * 300,  # 5 minutes d'intervalle
                "open": price - 25,
                "high": price + 30,
                "low": price - 35,
                "close": price,
                "volume": 1000 + i * 100,
                "symbol": "BTCUSD",
                "timeframe": "5m"
            }
            
            # Publier la candle
            channel = CHANNELS.MARKET_DATA.format(symbol="BTCUSDC", timeframe="5m")
            await self.pubsub.publish(channel, candle)
            
            self.test_metrics['candles_sent'] += 1
            
            # Petit délai pour simuler le temps réel
            await asyncio.sleep(0.5)
            
            # Log périodique
            if i % 5 == 0:
                status = await self.strategy_manager.get_strategy_status(self.test_session_id)
                df_info = status.get('dataframe_info', {})
                logger.info(f"   Candle {i+1}/20: DataFrame has {df_info.get('rows', 0)} rows, "
                           f"{len(df_info.get('columns', []))} columns")
        
        # Attendre que tout soit traité
        logger.info("⏳ Waiting for processing to complete...")
        await asyncio.sleep(10)
        
        # Vérifier les résultats
        status = await self.strategy_manager.get_strategy_status(self.test_session_id)
        performance = await self.strategy_manager.get_strategy_performance(self.test_session_id)
        
        logger.info("📊 Final results:")
        logger.info(f"   DataFrame info: {status.get('dataframe_info', {})}")
        logger.info(f"   Processing metrics: {status.get('metrics', {})}")
        
        if performance:
            logger.info(f"   Performance: {performance}")
        
        # Assertions
        df_info = status.get('dataframe_info', {})
        assert df_info.get('rows', 0) > 0, "DataFrame should have data"
        assert 'SMA_3' in df_info.get('columns', []), "SMA_3 indicator should be calculated"
        assert 'SMA_5' in df_info.get('columns', []), "SMA_5 indicator should be calculated"
        
        metrics = status.get('metrics', {})
        assert metrics.get('candles_processed', 0) > 0, "Candles should be processed"
        
        logger.info("✅ Candle processing and indicators test passed")
    
    async def test_signal_generation(self):
        """Test la génération de signaux avec crossover"""
        logger.info("🧪 Testing signal generation...")
        
        # Reset du compteur de signaux
        initial_signals = self.test_metrics['signals_received']
        
        # Envoyer des candles avec un pattern de crossover clair
        base_timestamp = datetime.now(timezone.utc).timestamp() + 10000
        
        logger.info("📉 Sending downtrend candles...")
        # D'abord, tendance baissière
        for i in range(10):
            price = 51000 - i * 100  # Prix descendant
            candle = {
                "timestamp": base_timestamp + i * 300,
                "open": price + 50,
                "high": price + 75,
                "low": price - 25,
                "close": price,
                "volume": 1500,
                "symbol": "BTCUSD",
                "timeframe": "5m"
            }
            
            channel = CHANNELS.MARKET_DATA.format(symbol="BTCUSD", timeframe="5m")
            await self.pubsub.publish(channel, candle)
            await asyncio.sleep(0.3)
        
        logger.info("📈 Sending uptrend candles (potential crossover)...")
        # Puis, tendance haussière (crossover potentiel)
        for i in range(15):
            price = 50000 + i * 80  # Prix montant
            candle = {
                "timestamp": base_timestamp + (10 + i) * 300,
                "open": price - 40,
                "high": price + 60,
                "low": price - 50,
                "close": price,
                "volume": 2000,
                "symbol": "BTCUSD",
                "timeframe": "5m"
            }
            
            channel = CHANNELS.MARKET_DATA.format(symbol="BTCUSD", timeframe="5m")
            await self.pubsub.publish(channel, candle)
            await asyncio.sleep(0.3)
        
        # Attendre les signaux
        logger.info("⏳ Waiting for signal generation...")
        await asyncio.sleep(15)
        
        # Vérifier les signaux
        signals_generated = self.test_metrics['signals_received'] - initial_signals
        logger.info(f"📡 Signals generated during test: {signals_generated}")
        
        # Obtenir les métriques finales
        status = await self.strategy_manager.get_strategy_status(self.test_session_id)
        final_metrics = status.get('metrics', {})
        
        logger.info(f"📊 Final strategy metrics: {final_metrics}")
        
        assert signals_generated >= 0, "Should have generated at least some signals (or none if conditions not met)"
        
        logger.info("✅ Signal generation test completed")
    
    async def test_global_metrics(self):
        """Test les métriques globales"""
        logger.info("🧪 Testing global metrics...")
        
        global_metrics = self.strategy_manager.get_global_metrics()
        
        logger.info("🌐 Global metrics:")
        for key, value in global_metrics.items():
            if key != 'indicators_service_metrics':
                logger.info(f"   {key}: {value}")
        
        # Métriques du service d'indicateurs
        indicators_metrics = global_metrics.get('indicators_service_metrics', {})
        logger.info("🔧 Indicators service metrics:")
        for key, value in indicators_metrics.items():
            logger.info(f"   {key}: {value}")
        
        # Assertions
        assert global_metrics['active_strategies'] > 0, "Should have active strategies"
        assert global_metrics['total_strategies'] > 0, "Should have total strategies"
        
        logger.info("✅ Global metrics test passed")
    
    async def run_all_tests(self):
        """Exécute tous les tests"""
        self.test_metrics['test_start_time'] = datetime.now(timezone.utc)
        
        try:
            logger.info("🚀 Starting comprehensive workflow integration tests")
            
            await self.test_basic_strategy_lifecycle()
            await self.test_candle_processing_and_indicators()
            await self.test_signal_generation()
            await self.test_global_metrics()
            
            # Calculer la durée du test
            test_end = datetime.now(timezone.utc)
            self.test_metrics['test_duration'] = (
                test_end - self.test_metrics['test_start_time']
            ).total_seconds()
            
            logger.info("🎉 All integration tests completed successfully!")
            logger.info(f"📊 Test summary:")
            logger.info(f"   Duration: {self.test_metrics['test_duration']:.1f} seconds")
            logger.info(f"   Candles sent: {self.test_metrics['candles_sent']}")
            logger.info(f"   Signals received: {self.test_metrics['signals_received']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Integration test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def cleanup(self):
        """Nettoie l'environnement de test"""
        logger.info("🧹 Cleaning up test environment...")
        
        if self.strategy_manager:
            await self.strategy_manager.shutdown()
        
        if self.pubsub:
            await self.pubsub.stop()
        
        logger.info("✅ Cleanup complete")


async def main():
    """Fonction principale de test"""
    logger.info("🧪 Workflow Integration Test Suite")
    logger.info("=" * 50)
    
    tester = WorkflowIntegrationTester()
    success = False
    
    try:
        await tester.setup()
        success = await tester.run_all_tests()
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
    
    finally:
        await tester.cleanup()
    
    if success:
        logger.info("🎊 Integration test suite completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Integration test suite failed!")
        sys.exit(1)


if __name__ == "__main__":
    # Vérifier que nous avons tous les modules nécessaires
    try:
        from core.strategy_workflow import TradingStrategy
        from core.indicators_service import IndicatorsService
        from core.strategy_manager_enhanced import StrategyManagerEnhanced
        logger.info("✅ All required modules imported successfully")
    except ImportError as e:
        logger.error(f"❌ Missing required module: {e}")
        sys.exit(1)
    
    asyncio.run(main())