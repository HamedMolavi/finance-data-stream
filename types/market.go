package types

import "time"

const (
	SPOT_MARKET    int32 = iota //
	FUTURES_MARKET              //
	FOREX_MARKET                //
	NOBITEX_MARKET              //
	BITYCLE_MARKET              //
)

const (
	SPOT_SYMBOL_MONTH_LISTING_URL   = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/spot/monthly/klines/%s/1m/"
	FUTURE_SYMBOL_MONTH_LISTING_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/%s/1m/"
	SPOT_WS_BASE_URL                = "wss://stream.binance.com:9443/stream?streams="
	FOREX_WS_BASE_URL               = "wss://data.tradingview.com/socket.io/websocket"
	NOBITEX_WS_BASE_URL             = "wss://ws.nobitex.ir/connection/websocket"
	BITYCLE_WS_BASE_URL             = "wss://streamer.bitycle.io/ws/market_data"
	FUTURES_WS_BASE_URL             = "wss://fstream.binance.com/stream?streams="
	SPOT_INFO_BASE_URL              = "https://api.binance.com/api/v3/exchangeInfo"
	FUTURES_INFO_BASE_URL           = "https://fapi.binance.com/fapi/v1/exchangeInfo"
	NOBITEX_INFO_BASE_URL           = "https://apiv2.nobitex.ir/market/stats?dstCurrency=irt"
	MAX_URL_LENGTH                  = 8000
	MAX_STREAM_PER_CONNECTION       = 1024
	HEADER_LINES                    = 5
	FOREX_CACHE_FILE                = "forex_symbols.json"
	SPOT_CACHE_FILE                 = "spot_symbols.json"
	FUTURES_CACHE_FILE              = "futures_symbols.json"
	NOBITEX_CACHE_FILE              = "nobitex_symbols.json"
	PRINT_INTERVAL                  = 1000 * time.Millisecond
)
