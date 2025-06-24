import asyncio
import urllib

import aiohttp
import ccxt
import websockets
import json
import time
import hmac
import hashlib
import base64
from urllib.parse import urlencode
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from interfaces.connector import Connector


class KrakenFutureConnector(Connector):
    def __init__(self, api_key: str = "", api_secret: str = "", sandbox: bool = False, futures: bool=True, tick_type:str='spot'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
        self.tick_type = tick_type
        if sandbox:
            self.base_url = "https://api.demo-futures.kraken.com"
            self.ws_url = "wss://demo-futures.kraken.com/ws/v1"
        elif futures:
            self.base_url = "https://futures.kraken.com"
        else:
            self.base_url = "https://api.kraken.com"
            self.ws_url = "wss://ws.kraken.com"
            
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_connection: Optional[websockets.WebSocketServerProtocol] = None
        self.subscriptions: Dict[str, Callable] = {}
        self.rate_limit_lock = asyncio.Semaphore(20)  # 20 requests per second
        self._ccxt = ccxt.kraken()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    async def __aenter__(self):
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.ws_connection:
            await self.ws_connection.close()
        # Petite pause pour permettre le nettoyage des ressources
        await asyncio.sleep(0.1)
    
    def _get_kraken_signature(self, urlpath: str, data: Dict) -> str:
        postdata = urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    async def _make_request_future(self, method: str, endpoint: str, params: Dict = None, private: bool = False) -> Dict:
        async with self.rate_limit_lock:
            url = f"{self.base_url}/{endpoint}"
            headers = {"User-Agent": "TradingBot/1.0"}

            if private and self.api_key and self.api_secret:
                nonce = str(int(1000 * time.time()))
                data = params or {}
                data['nonce'] = nonce

                headers['API-Key'] = self.api_key
                headers['API-Sign'] = self._get_kraken_signature(f"/0/{endpoint}", data)

                if method == "POST":
                    async with self.session.post(url, data=data, headers=headers) as response:
                        return await response.json()
                else:
                    async with self.session.get(url, params=data, headers=headers) as response:
                        return await response.json()
            else:
                if method == "POST":
                    async with self.session.post(url, data=params, headers=headers) as response:
                        return await response.json()
                else:
                    async with self.session.get(url, params=params, headers=headers) as response:
                        return await response.json()

    async def _make_request(self, method: str, endpoint: str, params: Dict = None, private: bool = False) -> Dict:
        async with self.rate_limit_lock:
            url = f"{self.base_url}/0/{endpoint}"
            headers = {"User-Agent": "TradingBot/1.0"}
            
            if private and self.api_key and self.api_secret:
                nonce = str(int(1000 * time.time()))
                data = params or {}
                data['nonce'] = nonce
                
                headers['API-Key'] = self.api_key
                headers['API-Sign'] = self._get_kraken_signature(f"/0/{endpoint}", data)
                
                if method == "POST":
                    async with self.session.post(url, data=data, headers=headers) as response:
                        return await response.json()
                else:
                    async with self.session.get(url, params=data, headers=headers) as response:
                        return await response.json()
            else:
                if method == "POST":
                    async with self.session.post(url, data=params, headers=headers) as response:
                        return await response.json()
                else:
                    async with self.session.get(url, params=params, headers=headers) as response:
                        return await response.json()
    
    # Public API methods
    async def get_server_time(self) -> Dict:
        return await self._make_request("GET", "public/Time")
    
    async def get_asset_info(self) -> Dict:
        return await self._make_request("GET", "public/Assets")
    
    async def get_tradable_pairs(self) -> Dict:
        return await self._make_request("GET", "public/AssetPairs")
    
    async def get_ticker(self, pair: str = None) -> Dict:
        params = {"pair": pair} if pair else {}
        return await self._make_request("GET", "public/Ticker", params)
    
    async def get_orderbook(self, pair: str, count: int = 100) -> Dict:
        params = {"pair": pair, "count": count}
        return await self._make_request("GET", "public/Depth", params)
    
    async def get_recent_trades(self, pair: str, since: str = None) -> Dict:
        params = {"pair": pair}
        if since:
            params["since"] = since
        return await self._make_request("GET", "public/Trades", params)
    
    #async def get_historical_ohlc(self, pair:str, interval: int, since: str, to: str ):
    #    return await self._ccxt.("GET", "public/OHLC", params)

    async def get_ohlc_future(self, symbol: str, interval: str, since: int = None, to: int = None, limit: int = 700) -> Dict:

        params = {}
        if since:
            params["from"] = since
        if to:
            params["to"] = to
        return await self._make_request_future("GET", f'''api/charts/v1/{self.tick_type}/PF_{symbol}/{interval}''', params)

    async def get_ohlc(self, pair: str, interval: int = 1, since: str = None, limit: int=700) -> Dict:
        params = {"pair": pair, "interval": interval}
        if since:
            params["since"] = since
            params["limit"] = limit
        return await self._make_request("GET", "public/OHLC", params)
    
    # Private API methods
    async def get_account_balance(self) -> Dict:
        return await self._make_request("POST", "private/Balance", private=True)
    
    async def get_open_orders(self) -> Dict:
        return await self._make_request("POST", "private/OpenOrders", private=True)
    
    async def get_closed_orders(self) -> Dict:
        return await self._make_request("POST", "private/ClosedOrders", private=True)
    
    async def get_trades_history(self) -> Dict:
        return await self._make_request("POST", "private/TradesHistory", private=True)
    
    async def place_order(
        self, 
        pair: str, 
        type_: str, 
        ordertype: str, 
        volume: str, 
        price: str = None,
        price2: str = None,
        leverage: str = None,
        validate: bool = False
    ) -> Dict:
        params = {
            "pair": pair,
            "type": type_,  # buy or sell
            "ordertype": ordertype,  # market, limit, stop-loss, etc.
            "volume": volume
        }
        
        if price:
            params["price"] = price
        if price2:
            params["price2"] = price2
        if leverage:
            params["leverage"] = leverage
        if validate:
            params["validate"] = "true"
            
        return await self._make_request("POST", "private/AddOrder", params, private=True)
    
    async def cancel_order(self, txid: str) -> Dict:
        params = {"txid": txid}
        return await self._make_request("POST", "private/CancelOrder", params, private=True)
    
    # WebSocket methods
    async def connect_websocket(self):
        try:
            self.ws_connection = await websockets.connect(self.ws_url)
            self.logger.info("WebSocket connected")
            asyncio.create_task(self._ws_message_handler())
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
    
    async def _ws_message_handler(self):
        try:
            async for message in self.ws_connection:
                data = json.loads(message)
                await self._process_ws_message(data)
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed")
        except Exception as e:
            self.logger.error(f"WebSocket error: {e}")
    
    async def _process_ws_message(self, data: Dict):
        if isinstance(data, list) and len(data) >= 2:
            channel_id = data[0]
            if channel_id in self.subscriptions:
                callback = self.subscriptions[channel_id]
                await callback(data)
    
    async def subscribe_ticker(self, pairs: List[str], callback: Callable):
        if not self.ws_connection:
            await self.connect_websocket()
        
        subscription = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "ticker"}
        }
        
        await self.ws_connection.send(json.dumps(subscription))
        
        # Store callback for processing
        for pair in pairs:
            self.subscriptions[f"ticker-{pair}"] = callback
    
    async def subscribe_orderbook(self, pairs: List[str], depth: int, callback: Callable):
        if not self.ws_connection:
            await self.connect_websocket()
        
        subscription = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "book", "depth": depth}
        }
        
        await self.ws_connection.send(json.dumps(subscription))
        
        for pair in pairs:
            self.subscriptions[f"book-{pair}"] = callback
    
    async def subscribe_trades(self, pairs: List[str], callback: Callable):
        if not self.ws_connection:
            await self.connect_websocket()
        
        subscription = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "trade"}
        }
        
        await self.ws_connection.send(json.dumps(subscription))
        
        for pair in pairs:
            self.subscriptions[f"trade-{pair}"] = callback
    
    async def disconnect_websocket(self):
        if self.ws_connection:
            await self.ws_connection.close()
            self.ws_connection = None
            self.subscriptions.clear()
            self.logger.info("WebSocket disconnected")