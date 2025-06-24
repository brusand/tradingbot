#!/usr/bin/env python3
import sys
import os
import asyncio

# Ajouter le répertoire parent au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from connectors.kraken_connector import KrakenConnector

class CandlesFetcher:
    """Classe pour récupérer des chandelles avec gestion intelligente des paramètres"""

    # Mapping des intervalles texte vers valeurs Kraken
    INTERVAL_MAP = {
        '1m': 1,      # 1 minute
        '5m': 5,      # 5 minutes  
        '15m': 15,    # 15 minutes
        '30m': 30,    # 30 minutes
        '1h': 60,     # 1 heure
        '4h': 240,    # 4 heures
        '1d': 1440,   # 1 jour
        '1w': 10080,  # 1 semaine
        '15d': 21600, # 15 jours
    }
    
    # Mapping pour convertir en secondes (pour les calculs)
    KRAKEN_TO_SECONDS = {
        1: 60,        # 1 minute = 60 secondes
        5: 5 * 60,    # 5 minutes = 300 secondes
        15: 15 * 60,  # etc.
        30: 30 * 60,
        60: 60 * 60,
        240: 4 * 60 * 60,
        1440: 24 * 60 * 60,
        10080: 7 * 24 * 60 * 60,
        21600: 15 * 24 * 60 * 60,
    }

    def __init__(self, connector):
        self.connector = connector
        self.max_candles_per_request = 700

    def _parse_interval(self, interval):
        """Parse l'intervalle et retourne (valeur_kraken, valeur_secondes)"""
        if isinstance(interval, str):
            interval_lower = interval.lower()
            if interval_lower in self.INTERVAL_MAP:
                kraken_val = self.INTERVAL_MAP[interval_lower]
                seconds_val = self.KRAKEN_TO_SECONDS[kraken_val]
                return kraken_val, seconds_val
            else:
                raise ValueError(f"Intervalle non supporté: {interval}")
        elif isinstance(interval, int):
            # Assume it's already a Kraken interval value
            if interval in self.KRAKEN_TO_SECONDS:
                return interval, self.KRAKEN_TO_SECONDS[interval]
            else:
                raise ValueError(f"Intervalle non supporté: {interval}")
        else:
            raise TypeError("L'intervalle doit être un string ou un entier")

    def _parse_date(self, date_input):
        """Parse différents formats de date et retourne un timestamp string"""
        if date_input is None:
            return None

        if isinstance(date_input, str):
            if date_input.isdigit():
                return date_input
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
                return str(int(dt.timestamp()))
            except ValueError:
                raise ValueError(f"Format de date non reconnu: {date_input}")
        elif hasattr(date_input, 'timestamp'):  # datetime
            return str(int(date_input.timestamp()))
        elif isinstance(date_input, int):
            return str(date_input)
        else:
            raise TypeError("La date doit être un string, datetime, int ou None")

    async def get_candles(self, pair, interval='1m', since=None, limit=None, end_time=None):
        """Récupère des chandelles avec gestion intelligente des paramètres"""
        interval_kraken, interval_seconds = self._parse_interval(interval)
        since_timestamp = self._parse_date(since)
        end_timestamp = self._parse_date(end_time)

        all_candles = []
        current_since = since_timestamp

        while True:
            response = await self.connector.get_ohlc(
                pair=pair,
                interval=interval_kraken,
                since=current_since,
            )

            # Extraire les données OHLC de la réponse Kraken
            if not response or 'result' not in response:
                print(f"❌ Pas de données dans la réponse: {response}")
                break
                
            # Kraken retourne les données sous format dict avec la paire comme clé
            candles_data = None
            for key, value in response['result'].items():
                if key != 'last' and isinstance(value, list):
                    candles_data = value
                    break
            
            if not candles_data:
                break
                
            # Convertir les données Kraken en format standardisé
            candles = []
            for ohlc in candles_data:
                if isinstance(ohlc, list) and len(ohlc) >= 6:
                    candles.append({
                        'timestamp': int(float(ohlc[0])),
                        'open': float(ohlc[1]),
                        'high': float(ohlc[2]),
                        'low': float(ohlc[3]),
                        'close': float(ohlc[4]),
                        'volume': float(ohlc[6])
                    })

            if not candles:
                break

            # Filtrer par end_time si spécifié
            if end_timestamp:
                candles = [c for c in candles if c.get('timestamp', 0) <= int(end_timestamp)]

            all_candles.extend(candles)

            # Vérifier si on a atteint la limite
            if limit and len(all_candles) >= limit:
                all_candles = all_candles[:limit]
                break

            # Si on a moins de chandelles que le maximum, on a tout récupéré
            if len(candles) < self.max_candles_per_request:
                break

            # Si on a spécifié end_time et qu'on l'a atteint
            if end_timestamp and candles and candles[-1].get('timestamp', 0) >= int(end_timestamp):
                break

            # Préparer la prochaine requête
            last_timestamp = candles[-1].get('timestamp', 0)
            current_since = str(last_timestamp + interval_seconds)

            # Petite pause pour éviter de surcharger l'API
            import time
            time.sleep(0.1)

        return all_candles

    async def get_recent_candles(self, pair, interval='1m', count=100):
        """Récupère les N dernières chandelles"""
        return await self.get_candles(pair=pair, interval=interval, limit=count)

async def test_fixed():
    async with KrakenConnector(sandbox=False) as connector:
        fetcher = CandlesFetcher(connector)
        
        # Test avec des paires et intervalles qui fonctionnent
        print("🔍 Test 1: XBTUSDT 1m (200 chandelles)")
        candles_1m = await fetcher.get_recent_candles('XBTUSDT', '1m', 200)
        print(f"✅ Récupéré: {len(candles_1m)} chandelles 1m")
        
        print("\n🔍 Test 2: XBTUSDT 5m (50 chandelles)")
        candles_5m = await fetcher.get_recent_candles('XBTUSDT', '5m', 50)
        print(f"✅ Récupéré: {len(candles_5m)} chandelles 5m")
        
        print("\n🔍 Test 3: XBTUSDT 1h (24 chandelles)")
        candles_1h = await fetcher.get_recent_candles('XBTUSDT', '1h', 24)
        print(f"✅ Récupéré: {len(candles_1h)} chandelles 1h")
        
        # Afficher quelques données pour vérifier
        if candles_5m:
            print(f"\n📊 Exemple de chandelle 5m:")
            print(f"   Timestamp: {candles_5m[0]['timestamp']}")
            print(f"   Open: {candles_5m[0]['open']}")
            print(f"   High: {candles_5m[0]['high']}")
            print(f"   Low: {candles_5m[0]['low']}")
            print(f"   Close: {candles_5m[0]['close']}")
            print(f"   Volume: {candles_5m[0]['volume']}")

if __name__ == '__main__':
    asyncio.run(test_fixed())