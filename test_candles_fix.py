#!/usr/bin/env python3
import sys
import os
import asyncio

# Ajouter le répertoire parent au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from connectors.kraken_connector import KrakenConnector

async def test_simple():
    async with KrakenConnector(sandbox=False) as connector:
        # Test simple avec paire et intervalle corrects
        print("🔍 Test simple avec XBTUSDT, interval=5 (5 minutes)")
        response = await connector.get_ohlc(
            pair='XBTUSDT',
            interval=5,  # 5 minutes en format Kraken
            since=None
        )
        
        print(f"Réponse: {response}")
        
        if response and 'result' in response:
            print("✅ Succès!")
            for key, value in response['result'].items():
                if key != 'last' and isinstance(value, list):
                    print(f"📊 Données pour {key}: {len(value)} chandelles")
                    if len(value) > 0:
                        print(f"   Première chandelle: {value[0]}")
                        print(f"   Dernière chandelle: {value[-1]}")
        else:
            print(f"❌ Erreur: {response}")

if __name__ == '__main__':
    asyncio.run(test_simple())