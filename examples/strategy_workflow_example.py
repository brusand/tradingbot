"""
Exemple d'utilisation du workflow de stratégie avec queue et interpréteur
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone
from typing import Dict

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.strategy_workflow import TradingStrategy, IndicatorConfig, SignalRule
from core.indicators_service import IndicatorsService
from core.pubsub_engine import PubSubEngine
from core.channels import CHANNELS
from data.models import StrategyConfig


# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockStrategyConfig:
    """Configuration de stratégie pour l'exemple"""
    
    def __init__(self):
        self.name = "EMA_RSI_Strategy"
        self.pairs = ["BTCUSD"]
        self.timeframe = "5m"
        self.max_dataframe_size = 500
        
        # Configuration des indicateurs
        self.indicators = {
            "EMA_50": IndicatorConfig(
                type="EMA",
                parameters={"period": 50},
                source="close"
            ),
            "EMA_100": IndicatorConfig(
                type="EMA", 
                parameters={"period": 100},
                source="close"
            ),
            "RSI_14": IndicatorConfig(
                type="RSI",
                parameters={"period": 14},
                source="close"
            ),
            "SMA_20": IndicatorConfig(
                type="SMA",
                parameters={"period": 20},
                source="close"
            )
        }
        
        # Règles de signaux avec interpréteur
        self.signal_rules = [
            # Signal LONG: EMA 50 croise au-dessus EMA 100 + RSI pas overbought
            SignalRule(
                name="ema_crossover_long",
                signal_type="LONG",
                condition="EMA_50 > EMA_100 & EMA_50[-1] <= EMA_100[-1] & RSI_14 < 70",
                priority=2
            ),
            
            # Signal SHORT: EMA 50 croise en-dessous EMA 100 + RSI pas oversold
            SignalRule(
                name="ema_crossover_short", 
                signal_type="SHORT",
                condition="EMA_50 < EMA_100 & EMA_50[-1] >= EMA_100[-1] & RSI_14 > 30",
                priority=2
            ),
            
            # Signal LONG simple: Prix au-dessus SMA + RSI oversold
            SignalRule(
                name="oversold_bounce",
                signal_type="LONG", 
                condition="close > SMA_20 & RSI_14 < 30 & RSI_14[-1] >= 30",
                priority=1
            ),
            
            # Signal de confirmation: EMA tendance haussière
            SignalRule(
                name="trend_confirmation",
                signal_type="LONG",
                condition="EMA_50 > EMA_50[-1] & EMA_100 > EMA_100[-1] & close > EMA_50",
                priority=1
            )
        ]


class MockMarketDataGenerator:
    """Générateur de données de marché pour les tests"""
    
    def __init__(self, pubsub: PubSubEngine):
        self.pubsub = pubsub
        self.is_running = False
        self.current_price = 50000.0
        self.timestamp = datetime.now(timezone.utc).timestamp()
        
    async def start_generating(self, interval: float = 2.0):
        """Démarre la génération de candles"""
        self.is_running = True
        
        while self.is_running:
            try:
                # Simuler une nouvelle candle
                candle = self._generate_candle()
                
                # Publier la candle
                channel = CHANNELS.MARKET_DATA.format(symbol="BTCUSD", timeframe="5m")
                await self.pubsub.publish(channel, candle)
                
                logger.info(f"Generated candle: {candle['close']:.2f} at {candle['timestamp']}")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error generating market data: {e}")
                await asyncio.sleep(1)
    
    def _generate_candle(self) -> Dict:
        """Génère une candle simulée"""
        import random
        
        # Variation aléatoire du prix
        variation = random.uniform(-100, 100)
        self.current_price += variation
        
        # Assurer que le prix reste positif
        self.current_price = max(1000, self.current_price)
        
        # Générer OHLCV
        open_price = self.current_price - random.uniform(-50, 50)
        close_price = self.current_price
        high_price = max(open_price, close_price) + random.uniform(0, 20)
        low_price = min(open_price, close_price) - random.uniform(0, 20)
        volume = random.uniform(1000, 10000)
        
        self.timestamp += 300  # 5 minutes
        
        return {
            "timestamp": self.timestamp,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "symbol": "BTCUSD",
            "timeframe": "5m"
        }
    
    def stop_generating(self):
        """Arrête la génération"""
        self.is_running = False


class MockAccountService:
    """Service de compte simulé"""
    
    def __init__(self, pubsub: PubSubEngine):
        self.pubsub = pubsub
        self.balance = 10000.0
        self.equity = 10000.0
        self.free_margin = 10000.0
        self.margin_used = 0.0
        
    async def start_service(self):
        """Démarre le service de compte"""
        await self.pubsub.subscribe(
            CHANNELS.PORTFOLIO_UPDATE.format(userId="default"),
            self.on_balance_request
        )
        logger.info("Mock account service started")
    
    async def on_balance_request(self, request_data: Dict, metadata):
        """Traite les demandes de balance"""
        action = request_data.get("action")
        if action != "get_current_balance":
            return
        
        strategy_id = request_data.get("strategy_id")
        candle_timestamp = request_data.get("candle_timestamp")
        
        # Simuler une petite variation de balance
        import random
        self.balance += random.uniform(-10, 10)
        self.equity = self.balance + random.uniform(-5, 5)
        
        # Publier la réponse
        response_data = {
            "strategy_id": strategy_id,
            "candle_timestamp": candle_timestamp,
            "balance": self.balance,
            "equity": self.equity,
            "free_margin": self.free_margin,
            "margin_used": self.margin_used,
            "timestamp": datetime.now(timezone.utc).timestamp()
        }
        
        await self.pubsub.publish(
            CHANNELS.PORTFOLIO_UPDATE.format(userId="default"),
            response_data
        )
        
        logger.debug(f"Sent balance update: {self.balance:.2f}")


async def main():
    """Fonction principale de démonstration"""
    logger.info("Starting Strategy Workflow Example")
    
    # 1. Initialiser PubSub
    pubsub = PubSubEngine()
    await pubsub.start()
    
    try:
        # 2. Démarrer le service d'indicateurs
        indicators_service = IndicatorsService(pubsub)
        await indicators_service.start_service()
        
        # 3. Démarrer le service de compte simulé
        account_service = MockAccountService(pubsub)
        await account_service.start_service()
        
        # 4. Créer et démarrer la stratégie
        strategy_config = MockStrategyConfig()
        strategy = TradingStrategy(strategy_config, pubsub)
        await strategy.start_strategy()
        
        # 5. Démarrer le générateur de données de marché
        market_generator = MockMarketDataGenerator(pubsub)
        generation_task = asyncio.create_task(market_generator.start_generating(interval=3.0))
        
        # 6. S'abonner aux signaux pour voir les résultats
        async def on_signal(signal_data: Dict, metadata):
            signal = signal_data.get("signal", {})
            logger.info(f"🚨 SIGNAL: {signal.get('type')} for {signal.get('symbol')} "
                       f"by rule '{signal.get('rule_name')}' - {signal.get('rule_condition')}")
            
            # Afficher le contexte du DataFrame
            context = signal.get('dataframe_snapshot', {})
            if context:
                logger.info(f"   Context: close={context.get('close', 'N/A'):.2f}, "
                           f"EMA_50={context.get('EMA_50', 'N/A'):.2f}, "
                           f"EMA_100={context.get('EMA_100', 'N/A'):.2f}, "
                           f"RSI_14={context.get('RSI_14', 'N/A'):.2f}")
        
        signal_channel = CHANNELS.STRATEGIES_SIGNAL.format(
            strategyId=strategy_config.name,
            symbol="BTCUSD"
        )
        await pubsub.subscribe(signal_channel, on_signal)
        
        # 7. Monitoring des métriques
        async def log_metrics():
            while True:
                await asyncio.sleep(30)
                
                # Métriques de la stratégie
                df_info = strategy.get_dataframe_info()
                logger.info(f"Strategy metrics: {strategy.processing_metrics}")
                logger.info(f"DataFrame info: {df_info}")
                
                # Métriques des indicateurs
                indicators_metrics = indicators_service.get_metrics()
                logger.info(f"Indicators metrics: {indicators_metrics}")
        
        metrics_task = asyncio.create_task(log_metrics())
        
        # 8. Laisser tourner pendant un moment
        logger.info("🚀 System running... Press Ctrl+C to stop")
        
        # Attendre 2 minutes pour voir plusieurs candles et signaux
        await asyncio.sleep(120)
        
        logger.info("Demo completed after 2 minutes")
        
    except KeyboardInterrupt:
        logger.info("Stopping due to user interrupt")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
    
    finally:
        # Nettoyage
        logger.info("Cleaning up...")
        
        market_generator.stop_generating()
        await strategy.stop_strategy()
        await indicators_service.stop_service()
        await pubsub.stop()
        
        logger.info("Example completed")


class StrategyWorkflowTester:
    """Testeur pour le workflow de stratégie"""
    
    def __init__(self):
        self.pubsub = None
        self.strategy = None
        self.indicators_service = None
        
    async def setup(self):
        """Configure l'environnement de test"""
        self.pubsub = PubSubEngine()
        await self.pubsub.start()
        
        self.indicators_service = IndicatorsService(self.pubsub)
        await self.indicators_service.start_service()
        
        strategy_config = MockStrategyConfig()
        self.strategy = TradingStrategy(strategy_config, self.pubsub)
        await self.strategy.start_strategy()
        
        logger.info("Test environment setup complete")
    
    async def test_single_candle_processing(self):
        """Test le traitement d'une seule candle"""
        logger.info("Testing single candle processing...")
        
        # Simuler une candle
        test_candle = {
            "timestamp": datetime.now(timezone.utc).timestamp(),
            "open": 50000.0,
            "high": 50100.0,
            "low": 49900.0,
            "close": 50050.0,
            "volume": 1000.0
        }
        
        # Envoyer la candle
        await self.strategy.on_new_candle(test_candle, {})
        
        # Attendre le traitement
        await asyncio.sleep(5)
        
        # Vérifier les résultats
        df_info = self.strategy.get_dataframe_info()
        logger.info(f"DataFrame after single candle: {df_info}")
        
        assert df_info['rows'] == 1, "DataFrame should have 1 row"
        logger.info("✅ Single candle test passed")
    
    async def test_indicator_calculation(self):
        """Test le calcul d'indicateurs"""
        logger.info("Testing indicator calculation...")
        
        # Envoyer plusieurs candles pour avoir assez de données
        base_price = 50000.0
        base_timestamp = datetime.now(timezone.utc).timestamp()
        
        for i in range(25):  # Assez pour calculer EMA_100
            candle = {
                "timestamp": base_timestamp + i * 300,
                "open": base_price + i * 10,
                "high": base_price + i * 10 + 50,
                "low": base_price + i * 10 - 50,
                "close": base_price + i * 10 + 25,
                "volume": 1000.0
            }
            
            await self.strategy.on_new_candle(candle, {})
            await asyncio.sleep(0.1)  # Petit délai entre les candles
        
        # Attendre que tous les indicateurs soient calculés
        await asyncio.sleep(10)
        
        df_info = self.strategy.get_dataframe_info()
        logger.info(f"DataFrame after 25 candles: {df_info}")
        
        # Vérifier que les indicateurs sont présents
        expected_indicators = ['EMA_50', 'EMA_100', 'RSI_14', 'SMA_20']
        for indicator in expected_indicators:
            assert indicator in df_info['columns'], f"Indicator {indicator} should be in DataFrame"
        
        logger.info("✅ Indicator calculation test passed")
    
    async def test_signal_generation(self):
        """Test la génération de signaux"""
        logger.info("Testing signal generation with interpreter...")
        
        # Configurer un écouteur de signaux
        signals_received = []
        
        async def signal_listener(signal_data: Dict, metadata):
            signals_received.append(signal_data)
            logger.info(f"Received signal: {signal_data.get('signal', {}).get('type')}")
        
        signal_channel = CHANNELS.STRATEGIES_SIGNAL.format(
            strategyId=self.strategy.config.name,
            symbol="BTCUSD"
        )
        await self.pubsub.subscribe(signal_channel, signal_listener)
        
        # Envoyer des candles avec un pattern de crossover
        base_timestamp = datetime.now(timezone.utc).timestamp()
        
        # D'abord, des candles où EMA 50 < EMA 100
        for i in range(60):
            price = 50000 - i * 20  # Prix descendant
            candle = {
                "timestamp": base_timestamp + i * 300,
                "open": price,
                "high": price + 10,
                "low": price - 10,
                "close": price,
                "volume": 1000.0
            }
            await self.strategy.on_new_candle(candle, {})
            await asyncio.sleep(0.1)
        
        # Puis, des candles où EMA 50 > EMA 100 (crossover)
        for i in range(20):
            price = 47000 + i * 50  # Prix montant
            candle = {
                "timestamp": base_timestamp + (60 + i) * 300,
                "open": price,
                "high": price + 20,
                "low": price - 20,
                "close": price,
                "volume": 1000.0
            }
            await self.strategy.on_new_candle(candle, {})
            await asyncio.sleep(0.1)
        
        # Attendre que les signaux soient générés
        await asyncio.sleep(10)
        
        logger.info(f"Signals received: {len(signals_received)}")
        for signal_data in signals_received:
            signal = signal_data.get('signal', {})
            logger.info(f"  - {signal.get('type')} by {signal.get('rule_name')}")
        
        logger.info("✅ Signal generation test completed")
    
    async def cleanup(self):
        """Nettoie l'environnement de test"""
        if self.strategy:
            await self.strategy.stop_strategy()
        if self.indicators_service:
            await self.indicators_service.stop_service()
        if self.pubsub:
            await self.pubsub.stop()
        
        logger.info("Test environment cleaned up")


async def run_tests():
    """Exécute tous les tests"""
    logger.info("🧪 Starting Strategy Workflow Tests")
    
    tester = StrategyWorkflowTester()
    
    try:
        await tester.setup()
        
        await tester.test_single_candle_processing()
        await tester.test_indicator_calculation()
        await tester.test_signal_generation()
        
        logger.info("🎉 All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
    
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(run_tests())
    else:
        asyncio.run(main())