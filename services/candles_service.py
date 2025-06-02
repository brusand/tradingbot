import asyncio
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, UTC

from core.pubsub_engine import PubSubEngine, global_pubsub


class CandlesService:
    """Service de calcul d'indicateurs techniques avec PubSub"""
    
    def __init__(self, pubsub: PubSubEngine = None):
        self.pubsub = pubsub or global_pubsub
        self.logger = logging.getLogger("CandlesService")

    async def start(self):
        """Démarre le service d'indicateurs"""
        await self.pubsub.start()
        
        # S'abonner aux demandes de calcul d'indicateurs
        await self.pubsub.subscribe(
            "candles.*.*",
            self.on_candles_request
        )
        
        self.logger.info("Candless service started")
    
    async def stop(self):
        """Arrête le service d'indicateurs"""
        await self.pubsub.stop()
        self.logger.info("Candless service stopped")
    
    async def on_candles_request(self, request_data: Dict, metadata: Dict):
        """Traite une demande de calcul d'indicateur"""
        try:
            # Extraire les paramètres de la requête
            pair = request_data.get("pair")
            tf = request_data.get("tf")
            since = request_data.get("since")
            to = request_data.get("to")
            strategy_id = request_data.get("strategy_id")
            request_id = request_data.get("request_id")
            
            # Validation
            if not all([pair, tf, since, to]):
                self.logger.error(f"Invalid calculation request: missing required fields")
                return


            # Récupérer lles chandellles
            #values = await self._calculate_indicator(indicator_type, df, parameters)
            candles=[]
            # Publier le résultat
            market_name = f'{pair}{tf}'
            await self._publish_result(
                market_name,
                candles,
                strategy_id,
                request_id,
                metadata.get("channel", "")
            )
            
        except Exception as e:
            self.logger.error(f"Error processing calculation request: {e}")
            self.stats["errors"] += 1
    
    async def _calculate_indicator(self, indicator_type: str, df: pd.DataFrame, parameters: Dict) -> List[float]:
        """Calcule un indicateur spécifique"""
        if indicator_type not in self.indicator_functions:
            raise ValueError(f"Unknown indicator type: {indicator_type}")
        
        calculator = self.indicator_functions[indicator_type]
        return await calculator(df, parameters)
    


    async def _publish_result(self, market_name: str, values: List[float],
                            strategy_id: str,
                            request_id: str, original_channel: str):
        """Publie le résultat du calcul"""
        # Extraire le canal de réponse du canal original
        if "live" in original_channel:
            response_channel = original_channel.replace("live", "result")
        else:
            response_channel = f"{original_channel}.result"
        
        result_data = {
            "market": market_name,
            "values": values,
            "strategy_id": strategy_id,
            "request_id": request_id,
            "calculated_at": datetime.now(UTC).isoformat(),
            "values_count": len(values)
        }
        
        await self.pubsub.publish(response_channel, result_data)
        
        self.logger.debug(f"Published result for {market_name} to {strategy_id}")

# Instance globale pour faciliter l'utilisation
global_candles_service = CandlesService()