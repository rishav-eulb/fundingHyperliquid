#!/usr/bin/env python3
"""
Hyperliquid Info Client
~~~~~~~~~~~~~~~~~~~~~~~

A comprehensive client for interacting with Hyperliquid's info API endpoints.
Provides methods for fetching market data, user information, and trading metadata.
"""

import logging
import requests
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class OrderInfo:
    """Order information from Hyperliquid API."""
    oid: int
    cloid: str
    coin: str
    side: str  # "B" for buy, "A" for sell
    sz: float
    limit_px: float
    reduce_only: bool
    timestamp: int


@dataclass
class FillInfo:
    """Fill information from Hyperliquid API."""
    oid: int
    cloid: str
    coin: str
    side: str
    sz: float
    px: float
    fee: float
    timestamp: int


@dataclass
class AssetContext:
    """Perpetual asset context information."""
    name: str
    mark_px: float
    funding: float
    open_interest: float
    oracle_px: float
    index_px: float
    prev_day_px: float
    day_24h_change: float
    day_24h_vol: float


class HyperliquidInfoClient:
    """
    Comprehensive client for Hyperliquid info endpoints.
    
    Handles all info endpoints with proper error handling and validation.
    """
    
    def __init__(
        self,
        base_url: str = "https://api.hyperliquid.xyz",
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_delay: float = 0.1
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'HyperliquidInfoClient/1.0'
        })
        
    def _make_request(
        self, 
        endpoint: str, 
        method: str = "POST", 
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic and error handling.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET/POST)
            data: Request body for POST requests
            params: Query parameters for GET requests
            
        Returns:
            API response as dictionary
            
        Raises:
            requests.RequestException: For HTTP errors
            ValueError: For invalid responses
        """
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Making {method} request to {url}")
                logger.debug(f"Request data: {data}")
                
                if method.upper() == "POST":
                    response = self.session.post(
                        url, 
                        json=data, 
                        timeout=self.timeout
                    )
                else:
                    response = self.session.get(
                        url, 
                        params=params, 
                        timeout=self.timeout
                    )
                
                response.raise_for_status()
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
                result = response.json()
                
                # Validate response structure
                if not isinstance(result, (dict, list)):
                    raise ValueError(f"Invalid response format: {result}")
                
                logger.debug(f"Request successful: {endpoint}")
                return result
                
            except requests.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Response parsing error: {e}")
                raise ValueError(f"Invalid response from {endpoint}: {e}")
    
    def get_meta(self) -> Dict[str, Any]:
        """
        Get meta information (universe and margin tables).
        This is a working endpoint that we can use to understand the API.
        
        Returns:
            Meta information including universe and margin tables
            
        Example:
            meta = client.get_meta()
            logger.info(f"Universe: {len(meta['universe'])} assets")
        """
        data = {"type": "meta"}
        
        response = self._make_request("/info", data=data)
        
        logger.info("Retrieved meta information")
        return response
    
    def get_universe(self) -> List[Dict[str, Any]]:
        """
        Get universe information (all available assets).
        
        Returns:
            List of universe assets
            
        Example:
            universe = client.get_universe()
            for asset in universe:
                logger.info(f"Asset: {asset['name']}")
        """
        meta = self.get_meta()
        return meta.get("universe", [])
    
    def get_margin_tables(self) -> List[Dict[str, Any]]:
        """
        Get margin tables information.
        
        Returns:
            List of margin tables
            
        Example:
            margin_tables = client.get_margin_tables()
            logger.info(f"Found {len(margin_tables)} margin tables")
        """
        meta = self.get_meta()
        return meta.get("marginTables", [])
    
    def get_perpetual_dexs(self) -> List[Dict[str, Any]]:
        """
        Retrieve all perpetual dexs.
        
        Returns:
            List of perpetual DEX information
            
        Example:
            dexs = client.get_perpetual_dexs()
            for dex in dexs:
                logger.info(f"DEX: {dex['name']} - {dex['full_name']}")
        """
        data = {"type": "perpDexs"}
        
        response = self._make_request("/info", data=data)
        
        logger.info("Retrieved perpetual DEXs")
        return response
    
    def get_meta_and_asset_contexts(self) -> List[Dict[str, Any]]:
        """
        7. Retrieve perpetuals asset contexts (includes mark price, current funding, open interest, etc.).
        
        Returns:
            List containing meta information and asset contexts
            
        Example:
            contexts = client.get_meta_and_asset_contexts()
            # First element is meta, second is asset contexts
            meta = contexts[0]
            asset_contexts = contexts[1]
        """
        data = {"type": "metaAndAssetCtxs"}
        
        response = self._make_request("/info", data=data)
        
        logger.info("Retrieved meta and asset contexts")
        return response
    
    def get_perpetuals_asset_contexts(self) -> List[Dict[str, Any]]:
        """
        Helper method to get just the asset contexts from metaAndAssetCtxs.
        
        Returns:
            List of asset contexts with mark price, funding, open interest, etc.
            
        Example:
            contexts = client.get_perpetuals_asset_contexts()
            for context in contexts:
                logger.info(f"{context['markPx']}: Mark ${context['markPx']}, Funding {context['funding']}%")
        """
        full_response = self.get_meta_and_asset_contexts()
        
        # The response is [meta, asset_contexts]
        if len(full_response) >= 2:
            return full_response[1]
        else:
            logger.warning("Unexpected response format for asset contexts")
            return []
    
    def get_user_perpetuals_account_summary(self, user: str, dex: str = "") -> Dict[str, Any]:
        """
        8. Retrieve user's perpetuals account summary.
        
        Args:
            user: User address (42-character hex string)
            dex: Perp dex name (optional, defaults to empty string for first dex)
            
        Returns:
            User's perpetuals account summary
            
        Example:
            summary = client.get_user_perpetuals_account_summary("0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384")
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "clearinghouseState",
            "user": user
        }
        
        if dex:
            data["dex"] = dex
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved perpetuals account summary for user {user}")
        return response
    
    def get_user_funding_history(
        self, 
        user: str, 
        start_time: int,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve a user's funding history.
        
        Args:
            user: User address (42-character hex string)
            start_time: Start time in milliseconds, inclusive
            end_time: End time in milliseconds, inclusive (optional, defaults to current time)
            
        Returns:
            List of funding history entries
            
        Example:
            history = client.get_user_funding_history(
                "0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384",
                start_time=1640995200000
            )
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "userFunding",
            "user": user,
            "startTime": start_time
        }
        
        if end_time is not None:
            data["endTime"] = end_time
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved funding history for user {user}")
        return response
    
    def get_user_non_funding_ledger_updates(
        self, 
        user: str, 
        start_time: int,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve a user's non-funding ledger updates (deposits, transfers, withdrawals).
        
        Args:
            user: User address (42-character hex string)
            start_time: Start time in milliseconds, inclusive
            end_time: End time in milliseconds, inclusive (optional, defaults to current time)
            
        Returns:
            List of non-funding ledger updates
            
        Example:
            updates = client.get_user_non_funding_ledger_updates(
                "0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384",
                start_time=1640995200000
            )
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "userNonFundingLedgerUpdates",
            "user": user,
            "startTime": start_time
        }
        
        if end_time is not None:
            data["endTime"] = end_time
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved non-funding ledger updates for user {user}")
        return response
    
    def get_funding_history(
        self, 
        coin: str,
        start_time: int,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve historical funding rates for a specific coin.
        
        Args:
            coin: Coin name (e.g., "ETH", "BTC")
            start_time: Start time in milliseconds, inclusive
            end_time: End time in milliseconds, inclusive (optional, defaults to current time)
            
        Returns:
            List of historical funding rates
            
        Example:
            history = client.get_funding_history(
                "ETH",
                start_time=1640995200000
            )
        """
        data = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time
        }
        
        if end_time is not None:
            data["endTime"] = end_time
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved funding history for {coin}")
        return response
    
    def get_predicted_funding_rates(self) -> List[List[Any]]:
        """
        9. Retrieve predicted funding rates for different venues.
        
        Returns:
            List of predicted funding rates by venue
            
        Example:
            rates = client.get_predicted_funding_rates()
            for coin_data in rates:
                coin = coin_data[0]
                venues = coin_data[1]
                for venue, rate_info in venues:
                    logger.info(f"{coin} on {venue}: {rate_info['fundingRate']}")
        """
        data = {"type": "predictedFundings"}
        
        response = self._make_request("/info", data=data)
        
        logger.info("Retrieved predicted funding rates")
        return response
    
    
    def get_active_asset_data(self, user: str, coin: str) -> Dict[str, Any]:
        """
        Retrieve User's Active Asset Data.
        
        Args:
            user: User address (42-character hex string)
            coin: Coin name (e.g., "ETH", "BTC")
            
        Returns:
            Active asset data for the user and coin
            
        Example:
            data = client.get_active_asset_data("0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384", "ETH")
            logger.info(f"Max trade sizes: {data['maxTradeSzs']}")
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "activeAssetData",
            "user": user,
            "coin": coin
        }
        
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved active asset data for user {user} and coin {coin}")
        return response
    
    def get_open_orders(self, user: str) -> List[OrderInfo]:
        """
        1. Retrieve user's open orders.
        
        Args:
            user: User address (42-character hex string)
            
        Returns:
            List of open orders
            
        Example:
            orders = client.get_open_orders("0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384")
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "openOrders",
            "user": user
        }
        
        response = self._make_request("/info", data=data)
        
        orders = []
        # Handle both list and dict responses
        if isinstance(response, list):
            order_data_list = response
        else:
            order_data_list = response.get("data", [])
            
        for order_data in order_data_list:
            try:
                order = OrderInfo(
                    oid=order_data.get("oid", 0),
                    cloid=order_data.get("cloid", ""),
                    coin=order_data.get("coin", ""),
                    side=order_data.get("side", ""),
                    sz=float(order_data.get("sz", 0)),
                    limit_px=float(order_data.get("limitPx", 0)),
                    reduce_only=order_data.get("reduceOnly", False),
                    timestamp=order_data.get("timestamp", 0)
                )
                orders.append(order)
            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to parse order: {e}")
                continue
                
        logger.info(f"Retrieved {len(orders)} open orders for user {user}")
        return orders
    
    def get_open_orders_with_frontend(self, user: str) -> Dict[str, Any]:
        """
        2. Retrieve user's open orders with additional frontend info.
        
        Args:
            user: User address (42-character hex string)
            
        Returns:
            Dictionary with open orders and frontend metadata
            
        Example:
            result = client.get_open_orders_with_frontend("0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384")
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "openOrdersWithFrontend",
            "user": user
        }
        
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved open orders with frontend info for user {user}")
        return response
    
    def get_user_fills(self, user: str) -> List[FillInfo]:
        """
        3. Retrieve user's fills.
        
        Args:
            user: User address (42-character hex string)
            
        Returns:
            List of user fills
            
        Example:
            fills = client.get_user_fills("0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384")
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "userFills",
            "user": user
        }
        
        response = self._make_request("/info", data=data)
        
        fills = []
        for fill_data in response.get("data", []):
            try:
                fill = FillInfo(
                    oid=fill_data.get("oid", 0),
                    cloid=fill_data.get("cloid", ""),
                    coin=fill_data.get("coin", ""),
                    side=fill_data.get("side", ""),
                    sz=float(fill_data.get("sz", 0)),
                    px=float(fill_data.get("px", 0)),
                    fee=float(fill_data.get("fee", 0)),
                    timestamp=fill_data.get("timestamp", 0)
                )
                fills.append(fill)
            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to parse fill: {e}")
                continue
                
        logger.info(f"Retrieved {len(fills)} fills for user {user}")
        return fills
    
    def get_user_fills_by_time(
        self, 
        user: str, 
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[FillInfo]:
        """
        4. Retrieve user's fills by time range.
        
        Args:
            user: User address (42-character hex string)
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            
        Returns:
            List of user fills within time range
            
        Example:
            fills = client.get_user_fills_by_time(
                "0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384",
                start_time=1640995200000,  # Jan 1, 2022
                end_time=1640998800000     # Jan 1, 2022 + 1 hour
            )
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "userFillsByTime",
            "user": user
        }
        
        if start_time is not None:
            data["startTime"] = start_time
        if end_time is not None:
            data["endTime"] = end_time
            
        response = self._make_request("/info", data=data)
        
        fills = []
        for fill_data in response.get("data", []):
            try:
                fill = FillInfo(
                    oid=fill_data.get("oid", 0),
                    cloid=fill_data.get("cloid", ""),
                    coin=fill_data.get("coin", ""),
                    side=fill_data.get("side", ""),
                    sz=float(fill_data.get("sz", 0)),
                    px=float(fill_data.get("px", 0)),
                    fee=float(fill_data.get("fee", 0)),
                    timestamp=fill_data.get("timestamp", 0)
                )
                fills.append(fill)
            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to parse fill: {e}")
                continue
                
        logger.info(f"Retrieved {len(fills)} fills for user {user} in time range")
        return fills
    
    def query_order_status(
        self, 
        oid: Optional[int] = None, 
        cloid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        5. Query order status by oid or cloid.
        
        Args:
            oid: Order ID (optional)
            cloid: Client Order ID (optional)
            
        Returns:
            Order status information
            
        Example:
            status = client.query_order_status(oid=12345)
            # or
            status = client.query_order_status(cloid="my_order_123")
        """
        if oid is None and cloid is None:
            raise ValueError("Either oid or cloid must be provided")
            
        data = {"type": "orderStatus"}
        
        if oid is not None:
            data["oid"] = oid
        if cloid is not None:
            data["cloid"] = cloid
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Queried order status for oid={oid}, cloid={cloid}")
        return response
    
    def get_user_historical_data(
        self, 
        user: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        6. Retrieve user's historical data.
        
        Args:
            user: User address (42-character hex string)
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            
        Returns:
            User's historical trading data
            
        Example:
            history = client.get_user_historical_data(
                "0x7d839aB133BF5cCf6D486F7f9fa6b7c3d4a56384",
                start_time=1640995200000
            )
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "userHistoricalData",
            "user": user
        }
        
        if start_time is not None:
            data["startTime"] = start_time
        if end_time is not None:
            data["endTime"] = end_time
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved historical data for user {user}")
        return response
    
    def get_asset_context_by_name(self, asset_name: str) -> Optional[Dict[str, Any]]:
        """
        Helper method to get asset context for a specific asset.
        
        Args:
            asset_name: Asset name (e.g., "HYPE", "ETH")
            
        Returns:
            Asset context for the specified asset, or None if not found
        """
        contexts = self.get_perpetuals_asset_contexts()
        
        for context in contexts:
            if context.get("name", "").upper() == asset_name.upper():
                return context
                
        logger.warning(f"Asset context not found for {asset_name}")
        return None
    
    def get_all_mids(self, dex: str = "") -> Dict[str, str]:
        """
        Retrieve mids for all coins.
        
        Note: If the book is empty, the last trade price will be used as a fallback.
        
        Args:
            dex: Perp dex name (optional, defaults to empty string for first perp dex)
                 Spot mids are only included with the first perp dex.
            
        Returns:
            Dictionary mapping coin names to their mid prices
            
        Example:
            mids = client.get_all_mids()
                    logger.info(f"BTC mid price: {mids.get('BTC', 'N/A')}")
        logger.info(f"ETH mid price: {mids.get('ETH', 'N/A')}")
        """
        data = {"type": "allMids"}
        
        if dex:
            data["dex"] = dex
            
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved mids for all coins (dex: {dex or 'default'})")
        return response
    
    def get_token_info(self, token_id: str) -> Dict[str, Any]:
        """
        Retrieve information about a specific token.
        
        Args:
            token_id: Token ID as 34-character hex string (e.g., "0x00000000000000000000000000000096")
            
        Returns:
            Token information including name, decimals, etc.
            
        Example:
            token_info = client.get_token_info("0x00000000000000000000000000000096")  # HYPE token
                    logger.info(f"Token name: {token_info.get('name', 'Unknown')}")
        logger.info(f"Decimals: {token_info.get('decimals', 0)}")
        """
        # Validate token_id format
        if not token_id.startswith('0x') or len(token_id) != 34:
            raise ValueError(f"Invalid token_id format: {token_id}. Expected 34-character hex string starting with 0x")
        
        data = {
            "type": "tokenDetails",
            "tokenId": token_id
        }
        
        response = self._make_request("/info", data=data)
        logger.info(f"Retrieved token info for token_id {token_id}")
        return response
    
    def get_spot_meta(self) -> Dict[str, Any]:
        """
        Get spot metadata including token mappings.
        
        Returns:
            Spot metadata with tokens and their mappings
            
        Example:
            meta = client.get_spot_meta()
            tokens = meta.get("tokens", [])
            for token in tokens:
                logger.info(f"Index {token['index']}: {token['tokenId']}")
        """
        data = {"type": "spotMeta"}
        
        response = self._make_request("/info", data=data)
        logger.info("Retrieved spot metadata")
        return response
    
    def get_user_portfolio(self, user: str) -> List[List[Any]]:
        """
        Query a user's portfolio data.
        
        Args:
            user: User address (42-character hex string)
            
        Returns:
            List of portfolio data entries with time periods and their corresponding data
            
        Example:
            portfolio = client.get_user_portfolio("0x19Dcc52B74693bF7f5E38554d27c86D5906eB2eC")
            for entry in portfolio:
                period = entry[0]  # e.g., "day", "week", "month", "allTime"
                data = entry[1]
                logger.info(f"{period}: Volume = {data.get('vlm', '0')}")
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "portfolio",
            "user": user
        }
        
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved portfolio data for user {user}")
        return response
    
    def get_user_positions(self, user: str) -> Dict[str, Any]:
        """
        Additional helper: Get user's current positions.
        
        Args:
            user: User address (42-character hex string)
            
        Returns:
            User's current positions
        """
        if not user or len(user) != 42 or not user.startswith('0x'):
            raise ValueError("Invalid user address format")
            
        data = {
            "type": "userState",
            "user": user
        }
        
        response = self._make_request("/info", data=data)
        
        logger.info(f"Retrieved user positions for {user}")
        return response
    
    def close(self) -> None:
        """Close the client session."""
        self.session.close()
        logger.info("HyperliquidInfoClient session closed")


