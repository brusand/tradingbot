import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import time

import asyncio


class CandlesFetcher:
    """Classe pour récupérer des chandelles avec gestion intelligente des paramètres"""

    # Mapping des intervalles texte vers secondes
    INTERVAL_MAP = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '30m': 30,
        '1h': 60,
        '2h': 240,
        '3h': 3 * 60,
        '4h': 4 * 60,
        '6h': 6 * 60,
        '8h': 8 * 60,
        '12h': 12 * 60,
        '1d': 24 * 60,
        '3d': 3 * 24 * 60,
        '1w': 7 * 24 * 60,
    }

    def __init__(self, connector):
        """
        Initialise le récupérateur de chandelles

        Args:
            connector: Instance du connecteur avec méthode get_ohlc
        """
        self.connector = connector
        self.max_candles_per_request = 700

    def _parse_interval(self, interval: Union[str, int]) -> int:
        """
        Parse l'intervalle et retourne la valeur en secondes

        Args:
            interval: Intervalle au format string ('1m', '5m', etc.) ou int (secondes)

        Returns:
            int: Intervalle en secondes
        """
        if isinstance(interval, str):
            interval_lower = interval.lower()
            if interval_lower in self.INTERVAL_MAP:
                return self.INTERVAL_MAP[interval_lower]
            else:
                raise ValueError(f"Intervalle non supporté: {interval}. "
                                 f"Intervalles disponibles: {list(self.INTERVAL_MAP.keys())}")
        elif isinstance(interval, int):
            return interval
        else:
            raise TypeError("L'intervalle doit être un string ou un entier")

    def _parse_date(self, date_input: Union[str, datetime, int, None]) -> Optional[str]:
        """
        Parse différents formats de date et retourne un timestamp string

        Args:
            date_input: Date au format string, datetime, timestamp int, ou None

        Returns:
            str ou None: Timestamp au format string ou None
        """
        if date_input is None:
            return None

        if isinstance(date_input, str):
            # Si c'est déjà un timestamp string
            if date_input.isdigit():
                return date_input
            # Si c'est une date ISO format
            try:
                dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
                return str(int(dt.timestamp()))
            except ValueError:
                raise ValueError(f"Format de date non reconnu: {date_input}")

        elif isinstance(date_input, datetime):
            return str(int(date_input.timestamp()))

        elif isinstance(date_input, int):
            return str(date_input)

        else:
            raise TypeError("La date doit être un string, datetime, int ou None")

    async def get_candles(self,
                          pair: str,
                          interval: Union[str, int] = '1m',
                          since: Union[str, datetime, int, None] = None,
                          limit: Optional[int] = None,
                          end_time: Union[str, datetime, int, None] = None) -> List[Dict]:
        """
        Récupère des chandelles avec gestion intelligente des paramètres

        Args:
            pair: Paire de trading (ex: 'BTC/USD')
            interval: Intervalle ('1m', '5m', '1h', etc. ou secondes)
            since: Date de début (string, datetime, timestamp ou None)
            limit: Nombre maximum de chandelles à récupérer
            end_time: Date de fin (optionnel)

        Returns:
            List[Dict]: Liste des chandelles OHLC
        """
        interval_seconds = self._parse_interval(interval)
        since_timestamp = self._parse_date(since)
        end_timestamp = self._parse_date(end_time)

        all_candles = []
        current_since = since_timestamp

        while True:
            # Récupérer les chandelles pour cette requête
            response = await self.connector.get_ohlc(
                pair=pair,
                interval=interval_seconds,
                since=current_since,
            )

            # Extraire les données OHLC de la réponse Kraken
            if not response or 'result' not in response:
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
            # Utiliser le timestamp de la dernière chandelle + intervalle
            last_timestamp = candles[-1].get('timestamp', 0)
            current_since = str(last_timestamp + interval_seconds)

            # Petite pause pour éviter de surcharger l'API
            time.sleep(0.1)

        return all_candles

    async def get_recent_candles(self, pair: str, interval: Union[str, int] = '1m', count: int = 100) -> List[Dict]:
        """
        Récupère les N dernières chandelles

        Args:
            pair: Paire de trading
            interval: Intervalle
            count: Nombre de chandelles à récupérer

        Returns:
            List[Dict]: Liste des dernières chandelles
        """
        return await self.get_candles(pair=pair, interval=interval, limit=count)

    async def get_candles_range(self,
                          pair: str,
                          interval: Union[str, int] = '1m',
                          start_date: Union[str, datetime, int] = None,
                          end_date: Union[str, datetime, int] = None,
                          historic: int = 0) -> Dict[str, List[Dict]]:
        """
        Récupère les chandelles pour une période donnée avec historique

        Args:
            pair: Paire de trading
            interval: Intervalle
            start_date: Date de début
            end_date: Date de fin (peut être 'now' pour maintenant)
            historic: Nombre de chandelles historiques à récupérer avant start_date

        Returns:
            Dict: {'historic': List[Dict], 'candles': List[Dict]}
                  ou List[Dict] si historic=0 (compatibilité)
        """
        interval_minutes = self._parse_interval(interval)

        # Traitement de end_date='now'
        if end_date == 'now':
            end_date = datetime.now()

        # Conversion des dates en datetime pour les calculs
        start_dt = self._convert_to_datetime(start_date) if start_date else None
        end_dt = self._convert_to_datetime(end_date) if end_date else None

        # Conversion en timestamps pour l'API
        start_timestamp = self._parse_date(start_date)
        end_timestamp = self._parse_date(end_date)

        historic_candles = []
        main_candles = []

        # Si historic est demandé, calculer la date de début historique
        if historic > 0:
            if start_date is None:
                raise ValueError("start_date est requis quand historic > 0")

            # Calculer la date de début historique
            historic_duration = timedelta(minutes=interval_minutes * historic)
            histo_start_date = start_dt - historic_duration
            histo_start_timestamp = str(int(histo_start_date.timestamp()))

            # Récupérer l'historique (avant start_date)
            historic_candles = await self._fetch_candles_with_batching(
                pair=pair,
                interval_minutes=interval_minutes,
                start_dt=histo_start_date,
                end_dt=start_dt,
                start_timestamp=histo_start_timestamp,
                end_timestamp=start_timestamp
            )

        # Récupérer les chandelles principales (depuis start_date jusqu'à end_date)
        if start_dt and end_dt:
            main_candles = await self._fetch_candles_with_batching(
                pair=pair,
                interval_minutes=interval_minutes,
                start_dt=start_dt,
                end_dt=end_dt,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp
            )
        else:
            # Fallback sur la méthode classique si pas de dates complètes
            main_candles = await self.get_candles(
                pair=pair,
                interval=interval_minutes,
                since=start_timestamp,
                end_time=end_timestamp
            )

        # Retourner selon le format demandé
        if historic > 0:
            return {
                'historic': historic_candles,
                'candles': main_candles
            }
        else:
            # Compatibilité avec l'ancienne version
            return {
                'historic': [],
                'candles': main_candles
            }
    def _convert_to_datetime(self, date_input: Union[str, datetime, int]) -> datetime:
        """
        Convertit une date en objet datetime

        Args:
            date_input: Date sous différents formats

        Returns:
            datetime: Objet datetime
        """
        if isinstance(date_input, datetime):
            return date_input
        elif isinstance(date_input, str):
            if date_input.isdigit():
                return datetime.fromtimestamp(int(date_input))
            else:
                return datetime.fromisoformat(date_input.replace('Z', '+00:00'))
        elif isinstance(date_input, int):
            return datetime.fromtimestamp(date_input)
        else:
            raise TypeError("Format de date non supporté")

    def _calculate_expected_candles(self, start_dt: datetime, end_dt: datetime, interval_minutes: int) -> int:
        """
        Calcule le nombre approximatif de chandelles entre deux dates

        Args:
            start_dt: Date de début
            end_dt: Date de fin
            interval_seconds: Intervalle en secondes

        Returns:
            int: Nombre approximatif de chandelles
        """
        duration_minutes = (end_dt - start_dt).total_seconds() / 60
        return int(duration_minutes / interval_minutes)

    async def _fetch_candles_with_batching(self,
                                     pair: str,
                                     interval_minutes: int,
                                     start_dt: datetime,
                                     end_dt: datetime,
                                     start_timestamp: str,
                                     end_timestamp: str) -> List[Dict]:
        """
        Récupère les chandelles avec gestion intelligente des lots

        Args:
            pair: Paire de trading
            interval_seconds: Intervalle en secondes
            start_dt: Date de début (datetime)
            end_dt: Date de fin (datetime)
            start_timestamp: Timestamp de début (string)
            end_timestamp: Timestamp de fin (string)

        Returns:
            List[Dict]: Liste des chandelles
        """
        # Calculer le nombre approximatif de chandelles
        expected_candles = self._calculate_expected_candles(start_dt, end_dt, interval_minutes)

        print(f"📊 Période: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')}")
        print(f"📈 Chandelles estimées: {expected_candles}")

        # Si moins de 700 chandelles, une seule requête
        if expected_candles <= self.max_candles_per_request:
            print("✅ Une seule requête suffisante")
            return await self.get_candles(
                pair=pair,
                interval=interval_minutes,
                since=start_timestamp,
                end_time=end_timestamp
            )

        # Sinon, découper en lots de 700 chandelles max
        print(f"🔄 Découpage en lots de {self.max_candles_per_request} chandelles")

        all_candles = []
        current_start_dt = start_dt
        batch_number = 1

        while current_start_dt < end_dt:
            # Calculer la fin de ce lot (700 chandelles max)
            batch_duration = timedelta(seconds=interval_minutes * self.max_candles_per_request)
            current_end_dt = min(current_start_dt + batch_duration, end_dt)

            # Convertir en timestamps
            current_start_timestamp = str(int(current_start_dt.timestamp()))
            current_end_timestamp = str(int(current_end_dt.timestamp()))

            print(
                f"  📦 Lot {batch_number}: {current_start_dt.strftime('%Y-%m-%d %H:%M')} → {current_end_dt.strftime('%Y-%m-%d %H:%M')}")

            # Récupérer les chandelles pour ce lot
            batch_candles = await self.get_candles(
                pair=pair,
                interval=interval_minutes,
                since=current_start_timestamp,
                end_time=current_end_timestamp,
                limit=self.max_candles_per_request
            )

            if batch_candles:
                all_candles.extend(batch_candles)
                print(f"    ✅ {len(batch_candles)} chandelles récupérées")
            else:
                print(f"    ⚠️  Aucune chandelle récupérée")
                break

            # Passer au lot suivant
            current_start_dt = current_end_dt
            batch_number += 1

            # Petite pause entre les requêtes
            time.sleep(0.1)

        print(f"🎯 Total récupéré: {len(all_candles)} chandelles")
        return all_candles


# Exemple d'utilisation
async def exemple_utilisation(connector):
    """Exemple d'utilisation de la classe CandlesFetcher"""

    fetcher = CandlesFetcher(connector)

    # Récupérer les 200 dernières chandelles 1m
    #candles_1m = await fetcher.get_recent_candles('XBTUSDT', '1m', 200)

    # Récupérer les chandelles 5m depuis une date
    #candles_1h = await fetcher.get_candles(
    #    pair='XBTUSDT',
    #    interval='1h',
    #    since='2025-06-18T00:00:00Z'
    #)

    # NOUVEAU: Récupérer avec historique
    # Récupère 200 chandelles historiques avant 2025-06-01 + toutes les chandelles depuis 2025-06-01 jusqu'à maintenant
    #result_with_historic = await fetcher.get_candles_range(
    #    pair='XBTUSDT',
    #    interval='1h',
    #    start_date='2025-06-01',
    #    end_date='now',
    #    historic=200
    #)

    # result_with_historic contient:
    # {
    #   'historic': [...],  # 200 chandelles avant 2025-06-01
    #   'candles': [...]    # chandelles depuis 2025-06-01 jusqu'à maintenant
    # }

    # Récupérer sans historique (comportement classique)
    candles_1h = await fetcher.get_candles_range(
        pair='XBTUSDT',
        interval='1h',
        start_date='2025-06-01',
        end_date='now'
    )

    result_with_historic = await fetcher.get_candles_range(
        pair='XBTUSDT',
        interval='1h',
        start_date='2025-06-01',
        end_date='now',
        historic=200
    )

    # Récupérer avec limite
    #limited_candles = await fetcher.get_candles(
    #    pair='XBTUSDT',
    #    interval='15m',
    #    limit=1000
    #)

    return {
        'candles_1m': 0, #len(candles_1m),
        'candles_5m': 0, #len(candles_5m),
        'historic_count': len(result_with_historic['historic']),
        'main_candles_count': 0,#len(result_with_historic['candles']),
        'candles_1h': len(candles_1h),
        'limited_candles': 0,#len(limited_candles)
    }

if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from connectors.kraken_connector import KrakenConnector
    from config.settings import settings
    async def main():
        async with KrakenConnector(settings.connector.api_key, settings.connector.api_secret, sandbox=False) as connector:
            return await exemple_utilisation(connector)
    
    result = asyncio.run(main())
    print("Résultats:", result)