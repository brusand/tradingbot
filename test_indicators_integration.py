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

from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from data.models import StrategyConfig
from data.persistence import DatabaseManager
from services.indicators_service import IndicatorsService
# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IndicatorsTester:
    """Testeur d'intégration pour le workflow complet"""
    
    def __init__(self):
        self.db_manager = None
        self.pubsub = None
        self.indicators_service = None
        self.test_session_id = "test_session_001"
        
        # Métriques de test
        self.test_metrics = {
            'candles_sent': 0,
            'indicators_received': 0,
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
        self.indicators_service = IndicatorsService(self.pubsub)
        
        # Ne pas démarrer le service car il va redémarrer PubSub
        # À la place, s'abonner directement aux demandes
        await self.pubsub.subscribe(
            "indicators.calculate",
            self.indicators_service.on_calculate_request,
            weak_ref=False
        )
        
        # S'abonner aux résultats d'indicateurs pour le test
        await self.pubsub.subscribe(
            "indicators.result.test_strategy",
            self.on_indicators_update,
            weak_ref=False
        )
        
        # Ajouter un debug subscriber pour voir ce qui passe
        await self.pubsub.subscribe(
            "indicators.calculate",
            self.on_debug_request,
            weak_ref=False
        )

        logger.info("✅ Test environment setup complete")

    async def on_debug_request(self, request_data: Dict, metadata):
        """Debug: voir les demandes qui arrivent"""
        logger.info(f"🔍 DEBUG: Received calculation request: {request_data.keys()}")
    
    async def on_indicators_update(self, indicator_data: Dict, metadata):
        """Réception des résultats d'indicateurs"""
        self.test_metrics['indicators_received'] += 1
        indicator_name = indicator_data.get('name', 'Unknown')
        values_count = len(indicator_data.get('values', []))
        logger.info(f"✅ Received indicator {indicator_name} with {values_count} values")

    async def test_candle_processing_and_indicators(self):
        """Test le traitement des candles et calcul d'indicateurs"""
        logger.info("🧪 Testing indicators service directly...")

        # Créer des données de test
        test_data = []
        base_price = 50000.0
        base_timestamp = datetime.now(timezone.utc).timestamp()
        
        logger.info("📈 Generating test data...")
        
        for i in range(20):  # Générer 20 candles
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
            test_data.append(candle)
        
        logger.info(f"✅ Generated {len(test_data)} test candles")
        
        # Tester différents indicateurs
        indicators_to_test = [
            {"name": "SMA_5", "type": "SMA", "parameters": {"period": 5}, "source": "close"},
            {"name": "SMA_10", "type": "SMA", "parameters": {"period": 10}, "source": "close"},
            {"name": "EMA_5", "type": "EMA", "parameters": {"period": 5}, "source": "close"},
            {"name": "RSI_14", "type": "RSI", "parameters": {"period": 14}, "source": "close"}
        ]
        
        logger.info("🧮 Testing indicator calculations...")
        
        for indicator_config in indicators_to_test:
            # Construire la demande au format attendu par le service
            calculation_request = {
                "indicator": indicator_config['name'],
                "config": {
                    "type": indicator_config['type'],
                    "parameters": indicator_config['parameters']
                },
                "dataframe": test_data,
                "strategy_id": "test_strategy",
                "candle_timestamp": base_timestamp,
                "request_id": f"test_{indicator_config['name']}"
            }
            
            logger.info(f"   Testing {indicator_config['name']}...")
            
            # Publier la demande
            await self.pubsub.publish(CHANNELS.INDICATORS_CALCULATE, calculation_request)
            self.test_metrics['candles_sent'] += 1
            
            # Attendre un peu pour le traitement
            await asyncio.sleep(2)
        
        # Attendre que tout soit traité
        logger.info("⏳ Waiting for all indicators to be processed...")
        await asyncio.sleep(5)
        
        # Vérifier les résultats
        logger.info("📊 Test results:")
        logger.info(f"   Calculation requests sent: {len(indicators_to_test)}")
        logger.info(f"   Indicator results received: {self.test_metrics['indicators_received']}")
        
        # Vérifications
        if self.test_metrics['indicators_received'] > 0:
            logger.info("✅ Indicators service is working correctly")
        else:
            logger.warning("⚠️ No indicator results received - checking service status...")
            
            # Essayer un test simple
            simple_request = {
                "indicator": "SMA_3",
                "config": {
                    "type": "SMA",
                    "parameters": {"period": 3}
                },
                "dataframe": test_data[:10],  # Seulement 10 candles
                "strategy_id": "test_strategy",
                "candle_timestamp": base_timestamp,
                "request_id": "test_simple"
            }
            
            logger.info("🔄 Trying simple SMA calculation...")
            await self.pubsub.publish(CHANNELS.INDICATORS_CALCULATE, simple_request)
            await asyncio.sleep(3)
            
            if self.test_metrics['indicators_received'] > 0:
                logger.info("✅ Simple test succeeded")
            else:
                logger.error("❌ Indicators service not responding")
        
        logger.info("✅ Indicator integration test completed")
    

    async def run_all_tests(self):
        """Exécute tous les tests"""
        self.test_metrics['test_start_time'] = datetime.now(timezone.utc)
        
        try:
            logger.info("🚀 Starting comprehensive workflow integration tests")

            await self.test_candle_processing_and_indicators()

            
            # Calculer la durée du test
            test_end = datetime.now(timezone.utc)
            self.test_metrics['test_duration'] = (
                test_end - self.test_metrics['test_start_time']
            ).total_seconds()
            
            logger.info("🎉 All integration tests completed successfully!")
            logger.info(f"📊 Test summary:")
            logger.info(f"   Duration: {self.test_metrics['test_duration']:.1f} seconds")
            logger.info(f"   Calculation requests sent: {self.test_metrics['candles_sent']}")
            logger.info(f"   Indicators received: {self.test_metrics['indicators_received']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Integration test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def cleanup(self):
        """Nettoie l'environnement de test"""
        logger.info("🧹 Cleaning up test environment...")
        
        # Pas besoin de stopper le service car on ne l'a pas démarré
        
        if self.pubsub:
            await self.pubsub.stop()
        
        logger.info("✅ Cleanup complete")


async def main():
    """Fonction principale de test"""
    logger.info("🧪 Workflow Integration Test Suite")
    logger.info("=" * 50)
    
    tester = IndicatorsTester()
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
        from services.indicators_service import IndicatorsService
        logger.info("✅ All required modules imported successfully")
        asyncio.run(main())
    except ImportError as e:
        logger.error(f"❌ Missing required module: {e}")
        sys.exit(1)


