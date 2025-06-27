#!/usr/bin/env python3
import logging
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import time
import yaml
import click
import pandas as pd
import os
import fcntl
import threading
from pathlib import Path
from contextlib import contextmanager
import tempfile
import shutil
import asyncio
import importlib

from config import settings

logger = logging.getLogger(__name__)
from config.settings import Settings
class CandlesFetcher:
    """Classe pour récupérer des chandelles avec gestion intelligente des paramètres et cache concurrent"""
    
    # Mapping des intervalles texte vers valeurs Kraken (en minutes)
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
    
    # Mapping pour convertir les intervalles Kraken (en minutes) en secondes (pour les calculs de temps)
    KRAKEN_TO_SECONDS = {
        '1m': 1 * 60,        # 1 minute = 60 secondes
        '5m': 5 * 60,    # 5 minutes = 300 secondes
        '15m': 15 * 60,  # 15 minutes = 900 secondes
        '30m': 30 * 60 ,  # 30 minutes = 1800 secondes
        '1h': 60 * 60,  # 60 minutes (1h) = 3600 secondes
        '4h': 4  * 60 * 60,    # 240 minutes (4h) = 14400 secondes
        '1d': 24  * 60 * 60,  # 1440 minutes (1d) = 86400 secondes
        '1w': 7 * 24 * 60 * 60,    # 10080 minutes (1w) = 604800 secondes
        '15d': 15 * 24 * 60 * 60,   # 21600 minutes (15d) = 1296000 secondes
    }
    
    def __init__(self, connector=None, cache_dir: str = "cache_candles", config_path: str = "config/exchanges_config.yaml"):
        """
        Initialise le récupérateur de chandelles
        
        Args:
            connector: Instance du connecteur avec méthode get_ohlc
            cache_dir: Répertoire pour stocker les fichiers cache feather
        """

        self.config_path = config_path
        self._load_configuration()
        self.connector = connector  # Connecteur legacy pour compatibilité
        self.max_candles_per_request = 700

        self.caches_dir = Path(cache_dir)
        self.caches_dir.mkdir(exist_ok=True)
        
        # Préparer les échanges sans initialiser les connecteurs encore
        for exchange in self.exchanges_registry:
            self.exchanges_registry[exchange]['cache_dir'] = Path(f'''{cache_dir}/{exchange}''')
            self.exchanges_registry[exchange]['cache_dir'].mkdir(exist_ok=True)
            self.exchanges_registry[exchange]['lock_dir'] = Path(f'''{cache_dir}/{exchange}/.locks''')
            self.exchanges_registry[exchange]['lock_dir'].mkdir(exist_ok=True)
            # Cache mémoire local pour éviter les relectures inutiles
            self.exchanges_registry[exchange]['memory_cache'] = {}
            self.exchanges_registry[exchange]['memory_cache_lock'] = threading.Lock()
            # Marquer comme non initialisé
            self.exchanges_registry[exchange]['connector'] = None
            self.exchanges_registry[exchange]['connector_initialized'] = False
            click.echo(f"⚠️ Exchange configuré: {exchange}")

        # Cache mémoire local pour éviter les relectures inutiles
        #self._memory_cache = {}
        #self._memory_cache_lock = threading.Lock()


    def _load_configuration(self):
        """Charge la configuration depuis YAML (CLI Adapted)"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    self.config = yaml.safe_load(f) or {}
                    self.exchanges_registry = self.config.get('exchanges', {})

            except Exception as e:
                click.echo(f"⚠️ Erreur de chargement config: {e}")
                # En cas d'erreur, initialiser avec des dictionnaires vides
                self.config = {}
                self.exchanges_registry = {}

    def load_connector(self, connector_class):
        try:
            module = importlib.import_module(f"connectors.{connector_class}")
            return getattr(module, f"{connector_class}")
        except (ImportError, AttributeError) as e:
            print(f"Erreur lors du chargement de la stratégie {connector_class} : {e}")
            return None
    
    async def __aenter__(self):
        """Initialise tous les connecteurs des exchanges avec context manager"""
        for exchange in self.exchanges_registry:
            if not self.exchanges_registry[exchange]['connector_initialized']:
                try:
                    connector_class = self.load_connector(self.exchanges_registry[exchange]['connector_class'])
                    api_key = os.getenv(f'''{self.exchanges_registry[exchange]['exchange']}_API_KEY''', "")
                    api_secret = os.getenv(f'''{self.exchanges_registry[exchange]['exchange']}_API_SECRET''', "")
                    
                    # Créer et initialiser le connecteur avec context manager
                    connector_instance = connector_class(
                        api_key=api_key, 
                        api_secret=api_secret, 
                        tick_type=self.exchanges_registry[exchange]['tick_type']
                    )
                    
                    # Initialiser le connecteur (équivalent à __aenter__)
                    if hasattr(connector_instance, '__aenter__'):
                        await connector_instance.__aenter__()
                    
                    self.exchanges_registry[exchange]['connector'] = connector_instance
                    self.exchanges_registry[exchange]['connector_initialized'] = True
                    click.echo(f"✅ Connecteur initialisé pour: {exchange}")
                    
                except Exception as e:
                    click.echo(f"❌ Erreur initialisation connecteur {exchange}: {e}")
                    self.exchanges_registry[exchange]['connector'] = None
                    self.exchanges_registry[exchange]['connector_initialized'] = False
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ferme proprement tous les connecteurs des exchanges"""
        for exchange in self.exchanges_registry:
            if self.exchanges_registry[exchange]['connector_initialized']:
                try:
                    connector = self.exchanges_registry[exchange]['connector']
                    if connector and hasattr(connector, '__aexit__'):
                        await connector.__aexit__(exc_type, exc_val, exc_tb)
                    
                    self.exchanges_registry[exchange]['connector'] = None
                    self.exchanges_registry[exchange]['connector_initialized'] = False
                    click.echo(f"🛑 Connecteur fermé pour: {exchange}")
                    
                except Exception as e:
                    click.echo(f"⚠️ Erreur fermeture connecteur {exchange}: {e}")
        
        # Pause pour permettre le nettoyage complet
        await asyncio.sleep(0.1)

    @contextmanager
    def _file_lock(self, exchange: str, cache_file: Path, timeout: float = 30.0):
        """
        Gestionnaire de contexte pour verrouiller un fichier cache
        
        Args:
            cache_file: Chemin vers le fichier cache
            timeout: Timeout en secondes pour acquérir le verrou
            
        Yields:
            bool: True si le verrou a été acquis
        """
        lock_file = self.exchanges_registry[exchange]['lock_dir']  / f"{cache_file.name}.lock"
        acquired = False
        lock_fd = None
        
        try:
            # Créer le fichier de verrou
            lock_fd = os.open(str(lock_file), os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
            
            # Essayer d'acquérir le verrou avec timeout
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    print(f"🔒 Verrou acquis: {cache_file.name}")
                    break
                except (IOError, OSError):
                    time.sleep(0.1)  # Attendre 100ms avant de réessayer
            
            if not acquired:
                logger.info(f"⏰ Timeout verrou: {cache_file.name}")
                
            yield acquired
            
        finally:
            if acquired and lock_fd:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    logger.info(f"🔓 Verrou libéré: {cache_file.name}")
                except (IOError, OSError):
                    pass
            
            if lock_fd:
                try:
                    os.close(lock_fd)
                except (IOError, OSError):
                    pass
            
            # Nettoyer le fichier de verrou
            try:
                if lock_file.exists():
                    lock_file.unlink()
            except (IOError, OSError):
                pass
    
    def _get_cache_filename(self, exchange: str, pair: str, interval: str) -> Path:
        """
        Génère le nom du fichier cache pour une paire et un intervalle
        
        Args:
            pair: Paire de trading (ex: 'BTC/USD')
            interval_minutes: Intervalle en minutes (valeur Kraken)
            
        Returns:
            Path: Chemin vers le fichier cache
        """
        # Nettoyer le nom de la paire pour le nom de fichier
        safe_pair = pair.replace('/', '_').replace('\\', '_')
        return self.exchanges_registry[exchange]['cache_dir'] / f"{safe_pair}_{interval}.feather"
    
    def _get_cache_key(self, pair: str, interval_minutes: int) -> str:
        """
        Génère une clé unique pour le cache mémoire
        
        Args:
            pair: Paire de trading
            interval_minutes: Intervalle en minutes (valeur Kraken)
            
        Returns:
            str: Clé unique
        """
        return f"{pair}_{interval_minutes}m"
    
    def _load_cache(self, exchange:str, pair: str, interval: str) -> Optional[pd.DataFrame]:
        """
        Charge le cache depuis le fichier feather avec gestion concurrente
        
        Args:
            pair: Paire de trading
            interval_minutes: Intervalle en minutes (valeur Kraken)
            
        Returns:
            pd.DataFrame ou None: DataFrame du cache ou None si pas de cache
        """
        cache_file = self._get_cache_filename(exchange, pair, interval)
        cache_key = self._get_cache_key(pair, interval)
        
        # Vérifier le cache mémoire d'abord
        with self.exchanges_registry[exchange]['memory_cache_lock']:
            if cache_key in  self.exchanges_registry[exchange]['memory_cache']:
                cached_data, file_mtime =  self.exchanges_registry[exchange]['memory_cache'][cache_key]
                
                # Vérifier si le fichier a été modifié depuis la mise en cache
                if cache_file.exists():
                    current_mtime = cache_file.stat().st_mtime
                    if current_mtime == file_mtime:
                        logger.info(f"📂 Cache mémoire utilisé: {len(cached_data)} chandelles")
                        return cached_data.copy()
                else:
                    # Le fichier n'existe plus, supprimer du cache mémoire
                    del  self.exchanges_registry[exchange]['memory_cache'][cache_key]
        
        # Charger depuis le fichier avec verrou
        if cache_file.exists():
            with self._file_lock(exchange, cache_file, timeout=10.0) as lock_acquired:
                if not lock_acquired:
                    logger.info(f"⚠️  Impossible d'acquérir le verrou pour {cache_file.name}, utilisation sans verrou")
                
                try:
                    df = pd.read_feather(cache_file)
                    if not df.empty and 'timestamp' in df.columns:
                        # S'assurer que le DataFrame est trié par timestamp
                        df = df.sort_values('timestamp').reset_index(drop=True)
                        
                        # Mettre en cache mémoire
                        with  self.exchanges_registry[exchange]['memory_cache_lock']:
                            file_mtime = cache_file.stat().st_mtime
                            self.exchanges_registry[exchange]['memory_cache'][cache_key] = (df.copy(), file_mtime)
                        
                        logger.info(f"📂 Cache fichier chargé: {len(df)} chandelles ({df['timestamp'].min()} → {df['timestamp'].max()})")
                        return df
                    else:
                        logger.info("⚠️  Cache vide ou format incorrect")
                        return None
                except Exception as e:
                    logger.info(f"❌ Erreur lors du chargement du cache: {e}")
                    return None
        else:
            logger.info("📂 Aucun cache trouvé")
            return None
    
    def _save_cache(self, exchange: str, pair: str, interval: str, df: pd.DataFrame):
        """
        Sauvegarde le cache dans un fichier feather avec gestion concurrente
        
        Args:
            pair: Paire de trading
            interval_minutes: Intervalle en minutes (valeur Kraken)
            df: DataFrame à sauvegarder
        """
        if df.empty:
            return
        
        cache_file = self._get_cache_filename(exchange, pair, interval)
        cache_key = self._get_cache_key(pair, interval)
        
        with self._file_lock(exchange, cache_file, timeout=15.0) as lock_acquired:
            if not lock_acquired:
                logger.info(f"⚠️  Impossible d'acquérir le verrou pour écriture: {cache_file.name}")
                return
            
            try:
                # S'assurer que le DataFrame est trié et dédup
                df_clean = df.sort_values('timestamp').drop_duplicates(subset=['timestamp']).reset_index(drop=True)
                
                # Écriture atomique avec fichier temporaire
                temp_file = cache_file.with_suffix('.tmp')
                df_clean.to_feather(temp_file)
                
                # Déplacement atomique
                shutil.move(str(temp_file), str(cache_file))
                
                # Mettre à jour le cache mémoire
                with  self.exchanges_registry[exchange]['memory_cache_lock']:
                    file_mtime = cache_file.stat().st_mtime
                    self.exchanges_registry[exchange]['memory_cache'][cache_key] = (df_clean.copy(), file_mtime)
                
                logger.info(f"💾 Cache sauvegardé: {len(df_clean)} chandelles → {cache_file}")
                
            except Exception as e:
                logger.info(f"❌ Erreur lors de la sauvegarde du cache: {e}")
                # Nettoyer le fichier temporaire en cas d'erreur
                temp_file = cache_file.with_suffix('.tmp')
                if temp_file.exists():
                    try:
                        temp_file.unlink()
                    except:
                        pass
    
    def _candles_to_dataframe(self, candles: List[Dict]) -> pd.DataFrame:
        """
        Convertit une liste de chandelles en DataFrame
        
        Args:
            candles: Liste des chandelles
            
        Returns:
            pd.DataFrame: DataFrame des chandelles
        """
        if not candles:
            return pd.DataFrame()
        
        df = pd.DataFrame(candles)
        
        # S'assurer que la colonne timestamp existe et est en int
        if 'timestamp' in df.columns:
            df['timestamp'] = df['timestamp'].astype(int)
        
        return df
    
    def _dataframe_to_candles(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convertit un DataFrame en liste de chandelles
        
        Args:
            df: DataFrame des chandelles
            
        Returns:
            List[Dict]: Liste des chandelles
        """
        if df.empty:
            return []
        
        return df.to_dict('records')
    
    def _get_cache_bounds(self, cache_df: pd.DataFrame) -> tuple:
        """
        Récupère les bornes du cache (première et dernière timestamp)
        
        Args:
            cache_df: DataFrame du cache
            
        Returns:
            tuple: (first_timestamp, last_timestamp) ou (None, None) si cache vide
        """
        if cache_df.empty:
            return None, None
        
        return int(cache_df['timestamp'].min()/1000), int(cache_df['timestamp'].max()/1000)
    
    def _parse_interval(self, interval: Union[str, int]) -> tuple[int, int]:
        """
        Parse l'intervalle et retourne (valeur_kraken_minutes, valeur_secondes)
        
        Args:
            interval: Intervalle au format string ('1m', '5m', etc.) ou int (valeur Kraken en minutes)
            
        Returns:
            tuple: (valeur_kraken_minutes, valeur_secondes)
        """
        if isinstance(interval, str):
            interval_lower = interval.lower()
            if interval_lower in self.INTERVAL_MAP:
                kraken_val = self.INTERVAL_MAP[interval_lower]
                seconds_val = self.KRAKEN_TO_SECONDS[interval_lower]
                return kraken_val, seconds_val
            else:
                raise ValueError(f"Intervalle non supporté: {interval}. "
                               f"Intervalles disponibles: {list(self.INTERVAL_MAP.keys())}")
        elif isinstance(interval, int):
            # Assume it's already a Kraken interval value in minutes
            if interval in self.KRAKEN_TO_SECONDS:
                return interval, self.KRAKEN_TO_SECONDS[interval]
            else:
                raise ValueError(f"Intervalle non supporté: {interval}")
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
                   exchange: str='kraken_future_spot',
                   pair: str= "XBTUSD",
                   interval: Union[str, int] = '1m',
                   since: Union[str, datetime, int, None] = None,
                   limit: Optional[int] = None,
                   end_time: Union[str, datetime, int, None] = None) -> List[Dict]:
        """
        Récupère des chandelles avec gestion intelligente des paramètres
        
        Args:
            pair: Paire de trading (ex: 'BTC/USD')
            interval: Intervalle ('1m', '5m', '1h', etc. ou valeur Kraken en minutes)
            since: Date de début (string, datetime, timestamp ou None)
            limit: Nombre maximum de chandelles à récupérer
            end_time: Date de fin (optionnel)
            
        Returns:
            List[Dict]: Liste des chandelles OHLC
        """
        interval_kraken, interval_seconds = self._parse_interval(interval)
        since_timestamp = self._parse_date(since)
        end_timestamp = self._parse_date(end_time)
        
        all_candles = []
        current_since = since_timestamp
        
        while True:
            logger.info(f"GET CANDLES: {pair} {interval} {since} {self.timestamp_str(since_timestamp)} {end_time} {self.timestamp_str(end_timestamp)} ")
            # Récupérer les chandelles pour cette requête
            response = await self.exchanges_registry[exchange]['connector'].get_ohlc_future(
                symbol=pair,
                interval=interval,  # Utiliser la valeur Kraken
                since=current_since,
                to=end_timestamp
            )
            #logger.info(f"GET CANDLES RESPONSE: {response} ")
            # Extraire les données OHLC de la réponse Kraken
            if not response:
                break
            response_candles = []
            if 'result' not in response:
                 if 'candles' in response:
                     response_candles = response.items()
                 else:
                     break
            else:
                if 'error' in response:
                    logger.info(f"❌ Erreur API: {response['error']}")
                    break
                else:
                    response_candles = response['result'].items()
            # Kraken retourne les données sous format dict avec la paire comme clé
            candles_data = None
            for key, value in response_candles:
                logger.info(f"GET CANDLES RESPONSE RESULT: {key} {len(value)}")
                if key != 'last' and isinstance(value, list):
                    candles_data = value
                    #logger.info(f"GET CANDLES RESPONSE RESULT VALUE: {key} {candles_data}")
                    break
            
            if not candles_data:
                break
                
            # Convertir les données Kraken en format standardisé
            candles = []
            for ohlc in candles_data:
                #logger.info(f"GET CANDLES RESPONSE RESULT VALUE OHLC: {ohlc} {ohlc['time']}")#self.timestamp_str(int(float(ohlc['time'])))}")
                #if isinstance(ohlc, list) and len(ohlc) >= 6:
                candles.append({
                    'timestamp': int(float(ohlc['time'])),
                    'open': float(ohlc['open']),
                    'high': float(ohlc['high']),
                    'low': float(ohlc['low']),
                    'close': float(ohlc['close']),
                    'volume': float(ohlc['volume'])
                })
            
            if not candles:
                break
            
            # Filtrer par end_time si spécifié
            if end_timestamp:
                candles = [c for c in candles if c.get('timestamp', 0) / 1000 <= int(end_timestamp)]
            
            all_candles.extend(candles)
            
            # Vérifier si on a atteint la limite
            if limit and len(all_candles) >= limit:
                all_candles = all_candles[:limit]
                break
            

            # Si on a spécifié end_time et qu'on l'a atteint
            if end_timestamp and candles and all_candles[-1].get('timestamp', 0) /1000 >= int(end_timestamp):
                break
            
            # Préparer la prochaine requête
            # Utiliser le timestamp de la dernière chandelle + intervalle
            last_timestamp = int(all_candles[-1].get('timestamp', 0) / 1000)
            last_current_since = current_since
            current_since = str(last_timestamp + interval_seconds)
            logger.info(
                f"GET CANDLES RESPONSE MORE SINCE : last {last_current_since} next {int(current_since)}")
            # Si on a moins de chandelles que le maximum, on a tout récupéré
            if not bool(response['more_candles']):
                break
            # Petite pause pour éviter de surcharger l'API
            time.sleep(0.1)
        
        return all_candles
    
    def timestamp_str(self, _timestamp):
        if _timestamp is not None:
            return datetime.fromtimestamp(float(_timestamp)).strftime("%m/%d/%Y, %H:%M:%S")
        return ""
    async def get_recent_candles(self, exchange: str, pair: str, interval: Union[str, int] = '1m', count: int = 100) -> List[Dict]:
        """
        Récupère les N dernières chandelles
        
        Args:
            pair: Paire de trading
            interval: Intervalle
            count: Nombre de chandelles à récupérer
            
        Returns:
            List[Dict]: Liste des dernières chandelles
        """
        return await self.get_candles(exchange, pair=pair, interval=interval, limit=count)

    async def get_recent_candles(self, exchange: str, pair: str, interval: Union[str, int] = '1m', since: str = '') -> List[Dict]:
        """
        Récupère les N dernières chandelles

        Args:
            pair: Paire de trading
            interval: Intervalle
            count: Nombre de chandelles à récupérer

        Returns:
            List[Dict]: Liste des dernières chandelles
        """
        return await self.get_candles(exchange=exchange, pair=pair, interval=interval, since=since)

    async def _fetch_missing_data(self, exchange: str, pair: str, interval: str,
                           start_timestamp: Optional[int], end_timestamp: Optional[int],
                           cache_first: Optional[int], cache_last: Optional[int]) -> List[Dict]:
        """
        Récupère les données manquantes en fonction du cache
        
        Args:
            pair: Paire de trading
            interval: Intervalle en str (valeur Kraken)
            start_timestamp: Timestamp de début demandé
            end_timestamp: Timestamp de fin demandé
            cache_first: Premier timestamp du cache
            cache_last: Dernier timestamp du cache
            
        Returns:
            List[Dict]: Liste des chandelles manquantes
        """
        missing_candles = []
        interval_seconds = self.KRAKEN_TO_SECONDS[interval]
        
        # Cas 1: Données avant le cache
        if start_timestamp and cache_first and start_timestamp < cache_first:
            logger.info(f"🔍 Récupération avant cache: {start_timestamp} → {cache_first}")
            before_candles = await self._fetch_candles_with_batching_no_cache(
                exchange= exchange,
                pair=pair,
                interval=interval,
                start_dt=datetime.fromtimestamp(start_timestamp),
                end_dt=datetime.fromtimestamp(cache_first),
                start_timestamp=str(start_timestamp),
                end_timestamp=str(cache_first - interval_seconds)  # Éviter le doublon
            )
            missing_candles.extend(before_candles)
        
        # Cas 2: Données après le cache
        if end_timestamp and cache_last and end_timestamp > cache_last + interval_seconds:
            logger.info(f"🔍 Récupération après cache: {cache_last} → {end_timestamp}")
            after_candles = await self._fetch_candles_with_batching_no_cache(
                exchange=exchange,
                pair=pair,
                interval=interval,
                start_dt=datetime.fromtimestamp(cache_last + interval_seconds),  # Éviter le doublon
                end_dt=datetime.fromtimestamp(end_timestamp),
                start_timestamp=str(cache_last + interval_seconds),
                end_timestamp=str(end_timestamp)
            )
            missing_candles.extend(after_candles)
        
        # Cas 3: Pas de cache, récupérer tout
        if cache_first is None and cache_last is None:
            logger.info(f"🔍 Récupération complète (pas de cache): {start_timestamp} → {end_timestamp}")
            if start_timestamp and end_timestamp:
                all_candles = await self._fetch_candles_with_batching_no_cache(
                    exchange= exchange,
                    pair=pair,
                    interval=interval,
                    start_dt=datetime.fromtimestamp(start_timestamp),
                    end_dt=datetime.fromtimestamp(end_timestamp),
                    start_timestamp=str(start_timestamp),
                    end_timestamp=str(end_timestamp)
                )
                missing_candles.extend(all_candles)
        
        return missing_candles
    
    async def get_candles_range(self,
                         exchange:str,
                         pair: str, 
                         interval: Union[str, int] = '1m',
                         start_date: Union[str, datetime, int] = None,
                         end_date: Union[str, datetime, int] = None,
                         historic: int = 0) -> Dict[str, List[Dict]]:
        """
        Récupère les chandelles pour une période donnée avec historique et cache
        
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
        interval_minutes, interval_seconds = self._parse_interval(interval)
        
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
            
            # Calculer la date de début historique (en minutes)
            historic_duration = timedelta(minutes=interval_minutes * historic)
            histo_start_date = start_dt - historic_duration
            histo_start_timestamp = str(int(histo_start_date.timestamp()))
            
            # Récupérer l'historique (avant start_date) avec cache
            historic_candles = await self._fetch_candles_with_batching(
                exchange=exchange,
                pair=pair,
                interval=interval,
                start_dt=histo_start_date,
                end_dt=start_dt,
                start_timestamp=histo_start_timestamp,
                end_timestamp=start_timestamp
            )
        
        # Récupérer les chandelles principales (depuis start_date jusqu'à end_date) avec cache
        if start_dt and end_dt:
            main_candles = await self._fetch_candles_with_batching(
                exchange=exchange,
                pair=pair,
                interval=interval,
                start_dt=start_dt,
                end_dt=end_dt,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp
            )
        else:
            # Fallback sur la méthode classique si pas de dates complètes
            main_candles = await self.get_candles(
                exchange=exchange,
                pair=pair,
                interval=interval,
                since=start_timestamp,
                end_time=end_timestamp
            )
        
        # Retourner selon le format demandé
        return {
            'historic': historic_candles[:-1],
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
    
    def _calculate_expected_candles(self, start_dt: datetime, end_dt: datetime, interval: str) -> int:
        """
        Calcule le nombre approximatif de chandelles entre deux dates
        
        Args:
            start_dt: Date de début
            end_dt: Date de fin
            interval_minutes: Intervalle en minutes (valeur Kraken)
            
        Returns:
            int: Nombre approximatif de chandelles
        """
        duration_minutes = (end_dt - start_dt).total_seconds() / 60
        return int(duration_minutes / self.INTERVAL_MAP[interval])
    
    async def _fetch_candles_with_batching(self,
                                   exchange:str,
                                   pair: str,
                                   interval: str,
                                   start_dt: datetime,
                                   end_dt: datetime,
                                   start_timestamp: str,
                                   end_timestamp: str) -> List[Dict]:
        """
        Récupère les chandelles avec gestion intelligente des lots et cache
        
        Args:
            exchange: Exchange configuré 
            pair: Paire de trading
            interval: Intervalle (string)
            start_dt: Date de début (datetime)
            end_dt: Date de fin (datetime)
            start_timestamp: Timestamp de début (string)
            end_timestamp: Timestamp de fin (string)
            
        Returns:
            List[Dict]: Liste des chandelles
        """
        start_ts = int(start_timestamp)
        end_ts = int(end_timestamp)
        interval_seconds = self.KRAKEN_TO_SECONDS[interval]
        
        logger.info(f"🔄 Fetching candles avec cache: {pair} {interval} {start_ts} → {end_ts}")
        
        # 1. Charger le cache existant dans cache_df
        cache_df = self._load_cache(exchange, pair, interval)
        cache_first, cache_last = self._get_cache_bounds(cache_df) if cache_df is not None else (None, None)
        
        logger.info(f"📂 Cache bounds: {cache_first} → {cache_last}")
        
        # 2. Charger les données manquantes AVANT la première chandelle du cache
        before_candles = []
        if start_ts < (cache_first or float('inf')):
            before_end = min(cache_first - interval_seconds, end_ts) if cache_first else end_ts
            if start_ts < before_end:
                logger.info(f"🔍 Récupération avant cache: {start_ts} → {before_end}")
                before_candles = await self._fetch_candles_with_batching_no_cache(
                    exchange=exchange,
                    pair=pair,
                    interval=interval,
                    start_dt=datetime.fromtimestamp(start_ts),
                    end_dt=datetime.fromtimestamp(before_end),
                    start_timestamp=str(start_ts),
                    end_timestamp=str(before_end)
                )
        
        # 3. Charger les données manquantes APRÈS la dernière chandelle du cache
        after_candles = []
        if end_ts > (cache_last or 0):
            after_start = max(cache_last + interval_seconds, start_ts) if cache_last else start_ts
            if after_start < end_ts:
                logger.info(f"🔍 Récupération après cache: {after_start} → {end_ts}")
                after_candles = await self._fetch_candles_with_batching_no_cache(
                    exchange=exchange,
                    pair=pair,
                    interval=interval,
                    start_dt=datetime.fromtimestamp(after_start),
                    end_dt=datetime.fromtimestamp(end_ts),
                    start_timestamp=str(after_start),
                    end_timestamp=str(end_ts)
                )
        
        # 4. Mettre à jour le cache avec toutes les nouvelles données
        if before_candles or after_candles:
            # Convertir les nouvelles données en DataFrame
            new_candles = before_candles + after_candles
            new_df = self._candles_to_dataframe(new_candles)
            
            if cache_df is not None and not cache_df.empty:
                # Fusionner avec le cache existant
                combined_df = pd.concat([cache_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Nettoyer et sauvegarder le cache mis à jour
            combined_df = combined_df.sort_values('timestamp').drop_duplicates(subset=['timestamp']).reset_index(drop=True)
            self._save_cache(exchange, pair, interval, combined_df)
            
            # Utiliser le cache mis à jour
            cache_df = combined_df
            logger.info(f"💾 Cache mis à jour avec {len(new_candles)} nouvelles chandelles")
        
        # 5. Retourner les données demandées depuis le cache complet
        all_candles = []
        if cache_df is not None and not cache_df.empty:
            # Filtrer les données dans la plage demandée
            # Convertir start_ts et end_ts en millisecondes pour matcher le cache
            start_ts_ms = start_ts * 1000
            end_ts_ms = end_ts * 1000
            
            mask = (cache_df['timestamp'] >= start_ts_ms) & (cache_df['timestamp'] <= end_ts_ms)
            filtered_df = cache_df[mask].copy()
            
            logger.info(f"📊 Filtrage: {start_ts_ms} ≤ timestamp ≤ {end_ts_ms}, trouvé {len(filtered_df)} chandelles")
            
            all_candles = self._dataframe_to_candles(filtered_df)
        
        logger.info(f"🎯 Total final: {len(all_candles)} chandelles")
        return all_candles
    
    async def _fetch_candles_with_batching_no_cache(self,
                                            exchange: str,
                                            pair: str,
                                            interval: str,
                                            start_dt: datetime,
                                            end_dt: datetime,
                                            start_timestamp: str,
                                            end_timestamp: str) -> List[Dict]:
        """
        Version sans cache pour récupération des données manquantes
        """
        # Calculer le nombre approximatif de chandelles
        expected_candles = self._calculate_expected_candles(start_dt, end_dt, interval)
        
        logger.info(f"📊 Période: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"📈 Chandelles estimées: {expected_candles}")
        
        # Si moins de 700 chandelles, une seule requête
        if expected_candles <= self.max_candles_per_request:
            print("✅ Une seule requête suffisante")
            return await self.get_candles(
                exchange=exchange,
                pair=pair,
                interval=interval,
                since=start_timestamp,
                end_time=end_timestamp
            )
        
        # Sinon, découper en lots de 700 chandelles max
        logger.info(f"🔄 Découpage en lots de {self.max_candles_per_request} chandelles")
        
        all_candles = []
        current_start_dt = start_dt
        batch_number = 1
        interval_seconds = self.KRAKEN_TO_SECONDS[interval]
        
        while current_start_dt < end_dt:
            # Calculer la fin de ce lot (700 chandelles max)
            batch_duration = timedelta(seconds=interval_seconds * self.max_candles_per_request)
            current_end_dt = min(current_start_dt + batch_duration, end_dt)
            
            # Convertir en timestamps
            current_start_timestamp = str(int(current_start_dt.timestamp()))
            current_end_timestamp = str(int(current_end_dt.timestamp()))
            
            logger.info(f"  📦 Lot {batch_number}: {current_start_dt.strftime('%Y-%m-%d %H:%M')} → {current_end_dt.strftime('%Y-%m-%d %H:%M')}")
            
            # Récupérer les chandelles pour ce lot
            batch_candles = await self.get_candles(
                exchange=exchange,
                pair=pair,
                interval=interval,
                since=current_start_timestamp,
                end_time=current_end_timestamp,
                limit=self.max_candles_per_request
            )
            logger.info(
                f"  📦 Lot {batch_number} responses : {len(batch_candles)}")

            if batch_candles:
                all_candles.extend(batch_candles)
                logger.info(f"    ✅ {len(batch_candles)} chandelles récupérées")
            else:
                logger.info(f"    ⚠️  Aucune chandelle récupérée")
                break
            
            # Passer au lot suivant
            current_start_dt = current_end_dt
            batch_number += 1
            
            # Petite pause entre les requêtes
            time.sleep(0.1)
        
        logger.info(f"🎯 Total récupéré: {len(all_candles)} chandelles")
        return all_candles
    
    def clear_cache(self, exchange:str, pair: Optional[str] = None, interval: Optional[Union[str, int]] = None):
        """
        Supprime le cache pour une paire/intervalle spécifique ou tout le cache
        
        Args:
            pair: Paire spécifique à supprimer (None = toutes les paires)
            interval: Intervalle spécifique à supprimer (None = tous les intervalles)
        """
        if pair and interval:
            # Supprimer un cache spécifique
            #interval_minutes, _ = self._parse_interval(interval)
            cache_file = self._get_cache_filename(exchange, pair, interval)
            cache_key = self._get_cache_key(pair, interval)
            
            with self._file_lock(exchange, cache_file, timeout=10.0) as lock_acquired:
                if lock_acquired and cache_file.exists():
                    cache_file.unlink()
                    logger.info(f"🗑️  Cache supprimé: {cache_file}")
                elif cache_file.exists():
                    logger.info(f"⚠️  Impossible de verrouiller {cache_file.name} pour suppression")
                else:
                    logger.info(f"📂 Aucun cache trouvé pour {pair} {interval}")
            
            # Supprimer du cache mémoire
            with self.exchanges_registry[exchange]['memory_cache_lock']:
                if cache_key in self.exchanges_registry[exchange]['memory_cache']:
                    del self.exchanges_registry[exchange]['memory_cache'][cache_key]
        else:
            # Supprimer tout le cache
            cache_files = list(self.exchanges_registry[exchange]['cache_dir'].glob("*.feather"))
            
            for cache_file in cache_files:
                with self._file_lock(exchange, cache_file, timeout=5.0) as lock_acquired:
                    if lock_acquired and cache_file.exists():
                        cache_file.unlink()
                        logger.info(f"🗑️  Cache supprimé: {cache_file}")
                    elif cache_file.exists():
                        logger.info(f"⚠️  Impossible de verrouiller {cache_file.name} pour suppression")
            
            # Vider le cache mémoire
            with self.exchanges_registry[exchange]['memory_cache_lock']:
                self.exchanges_registry[exchange]['memory_cache'].clear()
            
            logger.info("🧹 Nettoyage des caches terminé")
    
    def get_cache_info(self, exchange: str) -> Dict[str, Dict]:
        """
        Retourne des informations sur les caches existants
        
        Returns:
            Dict: Informations sur chaque cache
        """
        cache_info = {}
        
        for cache_file in self.exchanges_registry[exchange]['cache_dir'].glob("*.feather"):
            with self._file_lock(exchange, cache_file, timeout=5.0) as lock_acquired:
                if not lock_acquired:
                    cache_info[cache_file.name] = {'error': 'Fichier verrouillé par un autre processus'}
                    continue
                
                try:
                    df = pd.read_feather(cache_file)
                    if not df.empty and 'timestamp' in df.columns:
                        cache_info[cache_file.name] = {
                            'count': len(df),
                            'first_timestamp': int(df['timestamp'].min()/1000),
                            'last_timestamp': int(df['timestamp'].max()/1000),
                            'first_date': datetime.fromtimestamp(df['timestamp'].min()/1000).isoformat(),
                            'last_date': datetime.fromtimestamp(df['timestamp'].max()/1000).isoformat(),
                            'size_mb': cache_file.stat().st_size / (1024 * 1024),
                            'locked': False,
                            'validated': self.get_cache_validation(cache_file.name, df)
                        }
                    else:
                        cache_info[cache_file.name] = {'error': 'Cache vide ou format incorrect'}
                except Exception as e:
                    cache_info[cache_file.name] = {'error': str(e)}
        
        return cache_info
    
    def _candles_to_dataframe(self, candles: List[Dict]) -> pd.DataFrame:
        """
        Convertit une liste de chandelles en DataFrame
        
        Args:
            candles: Liste de chandelles
            
        Returns:
            pd.DataFrame: DataFrame avec colonnes standardisées
        """
        if not candles:
            return pd.DataFrame()
        
        return pd.DataFrame(candles)
    
    def _dataframe_to_candles(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convertit un DataFrame en liste de chandelles
        
        Args:
            df: DataFrame avec données de chandelles
            
        Returns:
            List[Dict]: Liste de chandelles
        """
        if df.empty:
            return []
        
        return df.to_dict('records')
    
    def get_cache_validation(self, cache_filename: str, df: pd.DataFrame) -> bool:
        """
        Valide l'intégrité du cache en vérifiant les doublons et la cohérence de timeframe
        
        Args:
            cache_filename: Nom du fichier cache (ex: "XBTUSD_1h.feather")
            df: DataFrame du cache à valider
            
        Returns:
            bool: True si le cache est valide, False sinon
        """
        try:
            # 1. Extraire la timeframe du nom de fichier
            # Format attendu : "PAIR_TIMEFRAME.feather" (ex: "XBTUSD_1h.feather")
            if not cache_filename.endswith('.feather'):
                logger.warning(f"⚠️ Format de fichier invalide: {cache_filename}")
                return False
            
            filename_without_ext = cache_filename.replace('.feather', '')
            parts = filename_without_ext.split('_')
            
            if len(parts) < 2:
                logger.warning(f"⚠️ Format de nom de fichier invalide: {cache_filename}")
                return False
            
            # La timeframe est le dernier élément après le dernier underscore
            timeframe = parts[-1]
            
            # Vérifier que la timeframe est valide
            if timeframe not in self.INTERVAL_MAP:
                logger.warning(f"⚠️ Timeframe invalide extraite du cache: {timeframe}")
                return False
            
            # 2. Vérifier que le DataFrame n'est pas vide et contient les colonnes nécessaires
            if df.empty:
                logger.warning(f"⚠️ Cache vide: {cache_filename}")
                return False
            
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"⚠️ Colonnes manquantes dans le cache {cache_filename}: {missing_columns}")
                return False
            
            # 3. Vérifier qu'il n'y a aucun doublon de timestamp
            duplicate_count = df['timestamp'].duplicated().sum()
            if duplicate_count > 0:
                logger.warning(f"⚠️ {duplicate_count} doublons de timestamp trouvés dans {cache_filename}")
                return False
            
            # 4. Vérifier que toutes les chandelles ont bien la même timeframe
            # Calculer l'intervalle attendu en millisecondes
            expected_interval_ms = self.KRAKEN_TO_SECONDS[timeframe] * 1000
            
            # Trier par timestamp pour calculer les écarts
            df_sorted = df.sort_values('timestamp')
            
            if len(df_sorted) > 1:
                # Calculer les écarts entre chandelles consécutives
                timestamp_diffs = df_sorted['timestamp'].diff().dropna()
                
                # Tolérance de ±5% pour les écarts (pour gérer les weekend/maintenances)
                tolerance = expected_interval_ms * 0.05
                min_allowed = expected_interval_ms - tolerance
                max_allowed = expected_interval_ms + tolerance
                
                # Vérifier que la majorité des écarts correspondent à la timeframe
                valid_intervals = timestamp_diffs[(timestamp_diffs >= min_allowed) & (timestamp_diffs <= max_allowed)]
                valid_ratio = len(valid_intervals) / len(timestamp_diffs)
                
                # Au moins 85% des écarts doivent être valides
                if valid_ratio < 0.85:
                    logger.warning(f"⚠️ Timeframe incohérente dans {cache_filename}: {valid_ratio:.1%} d'écarts valides (attendu: ≥85%)")
                    return False
                
                # Vérifier les écarts aberrants
                invalid_intervals = timestamp_diffs[(timestamp_diffs < min_allowed) | (timestamp_diffs > max_allowed)]
                if len(invalid_intervals) > 0:
                    # Log des premiers écarts aberrants pour debug
                    sample_invalid = invalid_intervals.head(3).tolist()
                    logger.debug(f"🔍 Écarts aberrants dans {cache_filename} (attendu: {expected_interval_ms}ms): {sample_invalid}")
            
            # 5. Vérifier que les données sont triées par timestamp
            if not df_sorted['timestamp'].equals(df['timestamp']):
                logger.warning(f"⚠️ Cache non trié par timestamp: {cache_filename}")
                return False
            
            # 6. Vérifier la cohérence des données OHLC
            invalid_ohlc = df[(df['high'] < df['low']) | 
                             (df['open'] < 0) | (df['high'] < 0) | 
                             (df['low'] < 0) | (df['close'] < 0) |
                             (df['volume'] < 0)]
            
            if len(invalid_ohlc) > 0:
                logger.warning(f"⚠️ {len(invalid_ohlc)} chandelles avec données OHLC invalides dans {cache_filename}")
                return False
            
            logger.info(f"✅ Cache validé avec succès: {cache_filename} ({len(df)} chandelles, timeframe: {timeframe})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la validation du cache {cache_filename}: {e}")
            return False
    
    def invalidate_memory_cache(self, exchange: str, pair: Optional[str] = None, interval: Optional[Union[str, int]] = None):
        """
        Invalide le cache mémoire pour forcer un rechargement depuis le fichier
        
        Args:
            exchange: Exchange concerné
            pair: Paire spécifique (None = tout invalidate)
            interval: Intervalle spécifique (None = tout invalidate)
        """
        with self.exchanges_registry[exchange]['memory_cache_lock']:
            if pair and interval:
                cache_key = self._get_cache_key(pair, interval)
                if cache_key in self.exchanges_registry[exchange]['memory_cache']:
                    del self.exchanges_registry[exchange]['memory_cache'][cache_key]
                    logger.info(f"🔄 Cache mémoire invalidé: {pair} {interval}")
            else:
                self.exchanges_registry[exchange]['memory_cache'].clear()
                logger.info("🔄 Tout le cache mémoire invalidé")


# Exemple d'utilisation
async def exemple_utilisation():
    """Exemple d'utilisation de la classe CandlesFetcher fusionnée"""
    
    # Utiliser le nouveau pattern avec context manager
    async with CandlesFetcher(cache_dir=f'''cache_crypto''') as fetcher:
        
        #logger.info("=== TEST DES FONCTIONNALITÉS FUSIONNÉES ===")
        
        # 🔥 Test 1: Récupération simple avec cache
        candles_1m = []
        #logger.info("\n=== TEST 1: Récupération avec cache ===")
        #candles_1m = await fetcher.get_candles('kraken_future_spot','XBTUSD', '1h', since='2025-01-01')
        #logger.info(f"✅ Récupéré: {len(candles_1m)} chandelles 1h")
        
        # 🔥 Test 2: Récupération avec historique et cache
        logger.info("\n=== TEST 2: Récupération avec historique et cache ===")
        result_with_historic = []
        result_with_historic = await fetcher.get_candles_range(
            exchange='kraken_future_spot',
            pair='XBTUSD',
            interval='1h',
            start_date='2025-01-01',
            end_date='now',
            historic=2
        )
        logger.info(f"✅ Récupéré: {len(result_with_historic['candles'])}  chandelles with historic {len(result_with_historic['historic'])} 1h")
        historic_count = len(result_with_historic['historic'])
        main_count = len(result_with_historic['candles'])
        
        # 🔥 Test 3: Informations cache
        logger.info("\n=== TEST 3: Informations cache ===")
        cache_info = []
        cache_info = fetcher.get_cache_info('kraken_future_spot')

        for filename, info in cache_info.items():
            if 'error' not in info:
                logger.info(f"📂 {filename}: {info['first_date']} -> {info['last_date']} valide {info['validated']} {info['count']} chandelles, {info['size_mb']:.2f} MB")
        
        return {
            'candles_1m': len(candles_1m),
            'historic_count': historic_count,
            'main_candles_count': main_count,
            'cache_files': cache_info
        }


if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from connectors.kraken_connector import KrakenConnector
    
    async def main():
        return await exemple_utilisation()
    
    result = asyncio.run(main())
    logger.info(f"Résultats: {result}")