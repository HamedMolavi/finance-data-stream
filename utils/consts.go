package utils

import (
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
)

const (
	SPOT_MARKET    int32 = iota //
	FUTURES_MARKET              //
	FOREX_MARKET                //
	NOBITEX_MARKET              //
	BITYCLE_MARKET              //
)

type SQL_MODE int

const (
	SQL_INSERT_MODE SQL_MODE = iota
	SQL_COPY_MODE
)

const (
	DATE_LAYOUT      = "2006-01-02"
	TIME_LAYOUT      = "15:04:05"
	DATE_TIME_LAYOUT = "2006-01-02 15:04:05"
)

var ALERT_TYPE_MAP = map[string]types.AlertTriggerType{
	"1":             types.AlertTriggerType("CROSS"),
	"2":             types.AlertTriggerType("CROSS_UP"),
	"3":             types.AlertTriggerType("CROSS_DOWN"),
	"4":             types.AlertTriggerType("GREATER"),
	"5":             types.AlertTriggerType("LESS"),
	"6":             types.AlertTriggerType("CHANNEL_ENTER"),
	"7":             types.AlertTriggerType("CHANNEL_LEAVE"),
	"8":             types.AlertTriggerType("CHANNEL_IN"),
	"9":             types.AlertTriggerType("CHANNEL_OUT"),
	"CROSS":         types.AlertTriggerType("CROSS"),
	"CROSS_UP":      types.AlertTriggerType("CROSS_UP"),
	"CROSS_DOWN":    types.AlertTriggerType("CROSS_DOWN"),
	"GREATER":       types.AlertTriggerType("GREATER"),
	"LESS":          types.AlertTriggerType("LESS"),
	"CHANNEL_ENTER": types.AlertTriggerType("CHANNEL_ENTER"),
	"CHANNEL_LEAVE": types.AlertTriggerType("CHANNEL_LEAVE"),
	"CHANNEL_IN":    types.AlertTriggerType("CHANNEL_IN"),
	"CHANNEL_OUT":   types.AlertTriggerType("CHANNEL_OUT"),
}

var ALERT_MARKET_MAP = map[string]int32{
	"nobitex":        NOBITEX_MARKET,
	"crypto spot":    SPOT_MARKET,
	"forex":          FOREX_MARKET,
	"crypto futures": FUTURES_MARKET,
	"cme futures":    FOREX_MARKET,
}

var CH_INTERVAL_TABLES = []map[types.Interval]string{
	{ // SPOT
		types.S1:  "one_second_spot_candles",
		types.S5:  "five_second_spot_candles",
		types.S15: "fifteen_second_spot_candles",
		types.S30: "thirty_second_spot_candles",
		types.M1:  "one_minute_spot_candles",
		types.M5:  "five_minute_spot_candles",
		types.M15: "fifteen_minute_spot_candles",
		types.M30: "thirty_minute_spot_candles",
		types.H1:  "one_hour_spot_candles",
		types.H4:  "four_hour_spot_candles",
		types.D1:  "one_day_spot_candles",
		types.W1:  "one_week_spot_candles",
		types.Mo1: "one_month_spot_candles",
	},
	{ // FUTURES
		types.S1:  "one_second_future_candles",
		types.S5:  "five_second_future_candles",
		types.S15: "fifteen_second_future_candles",
		types.S30: "thirty_second_future_candles",
		types.M1:  "one_minute_future_candles",
		types.M5:  "five_minute_future_candles",
		types.M15: "fifteen_minute_future_candles",
		types.M30: "thirty_minute_future_candles",
		types.H1:  "one_hour_future_candles",
		types.H4:  "four_hour_future_candles",
		types.D1:  "one_day_future_candles",
		types.W1:  "one_week_future_candles",
		types.Mo1: "one_month_future_candles",
	},
	{ // FOREX
		types.S1:  "one_second_forex_candles",
		types.S5:  "five_second_forex_candles",
		types.S15: "fifteen_second_forex_candles",
		types.S30: "thirty_second_forex_candles",
		types.M1:  "one_minute_forex_candles",
		types.M5:  "five_minute_forex_candles",
		types.M15: "fifteen_minute_forex_candles",
		types.M30: "thirty_minute_forex_candles",
		types.H1:  "one_hour_forex_candles",
		types.H4:  "four_hour_forex_candles",
		types.D1:  "one_day_forex_candles",
		types.W1:  "one_week_forex_candles",
		types.Mo1: "one_month_forex_candles",
	},
	{ // NOBITEX
		types.M1:  "one_minute_nobitex_candles",
		types.M5:  "five_minute_nobitex_candles",
		types.M15: "fifteen_minute_nobitex_candles",
		types.M30: "thirty_minute_nobitex_candles",
		types.H1:  "one_hour_nobitex_candles",
		types.H4:  "four_hour_nobitex_candles",
		types.D1:  "one_day_nobitex_candles",
		// types.W1: "one_week_nobitex_candles",
		// types.Mo1: "one_month_nobitex_candles",
	},
	{ // BITYCLE
		types.M1:  "one_minute_nobitex_candles",
		types.M5:  "five_minute_nobitex_candles",
		types.M15: "fifteen_minute_nobitex_candles",
		types.M30: "thirty_minute_nobitex_candles",
		types.H1:  "one_hour_nobitex_candles",
		types.H4:  "four_hour_nobitex_candles",
		types.D1:  "one_day_nobitex_candles",
		// types.W1: "one_week_nobitex_candles",
		// types.Mo1: "one_month_nobitex_candles",
	},
}

var SQL_INTERVAL_TABLES = []map[types.Interval]string{
	{ // SPOT
		types.S1:  "one_second_spot_candles",
		types.S5:  "five_second_spot_candles",
		types.S15: "fifteen_second_spot_candles",
		types.S30: "thirty_second_spot_candles",
		types.M1:  "one_minut_spot_candles",
		types.M5:  "five_minute_spot_candles",
		types.M15: "fifteen_minute_spot_candles",
		types.M30: "thirty_minute_spot_candles",
		types.H1:  "one_hour_spot_candles",
		types.H4:  "four_hour_spot_candles",
		types.D1:  "one_day_spot_candles",
		types.W1:  "one_week_spot_candles",
		types.Mo1: "one_month_spot_candles",
	},
	{ // FUTURES
		types.S1:  "one_second_future_candles",
		types.M1:  "one_minute_future_candles",
		types.M5:  "five_minute_future_candles",
		types.M15: "fifteen_minute_future_candles",
		types.M30: "thirty_minute_future_candles",
		types.H1:  "one_hour_future_candles",
		types.H4:  "four_hour_future_candles",
		types.D1:  "one_day_future_candles",
		types.W1:  "one_week_future_candles",
		types.Mo1: "one_month_future_candles",
	},
	{ // FOREX
		types.S1:  "one_second_forex_candles",
		types.S5:  "five_second_forex_candles",
		types.S15: "fifteen_second_forex_candles",
		types.S30: "thirty_second_forex_candles",
		types.M1:  "one_minute_forex_candles",
		types.M5:  "five_minute_forex_candles",
		types.M15: "fifteen_minute_forex_candles",
		types.M30: "thirty_minute_forex_candles",
		types.H1:  "one_hour_forex_candles",
		types.H4:  "four_hour_forex_candles",
		types.D1:  "one_day_forex_candles",
		types.W1:  "one_week_forex_candles",
		types.Mo1: "one_month_forex_candles",
	},
	{ // NOBITEX
		// types.S1: "one_second_nobitex_candles",
		types.M1:  "one_minute_nobitex_candles",
		types.M5:  "five_minute_nobitex_candles",
		types.M15: "fifteen_minute_nobitex_candles",
		types.M30: "thirty_minute_nobitex_candles",
		types.H1:  "one_hour_nobitex_candles",
		types.H4:  "four_hour_nobitex_candles",
		types.D1:  "one_day_nobitex_candles",
		// types.W1: "one_week_nobitex_candles",
		// types.Mo1: "one_month_nobitex_candles",
	},
	{ // BITYCLE
		// types.S1: "one_second_nobitex_candles",
		types.M1:  "one_minute_nobitex_candles",
		types.M5:  "five_minute_nobitex_candles",
		types.M15: "fifteen_minute_nobitex_candles",
		types.M30: "thirty_minute_nobitex_candles",
		types.H1:  "one_hour_nobitex_candles",
		types.H4:  "four_hour_nobitex_candles",
		types.D1:  "one_day_nobitex_candles",
		types.W1:  "one_week_nobitex_candles",
		// types.Mo1: "one_month_nobitex_candles",
	},
}
var INTERVAL_S_64 = map[types.Interval]int64{
	types.S1:  1,        // second
	types.S5:  5,        // second
	types.S15: 15,       // second
	types.S30: 30,       // second
	types.M1:  1 * 60,   // minute
	types.M5:  5 * 60,   // minute
	types.M15: 15 * 60,  // minute
	types.M30: 30 * 60,  // minute
	types.H1:  1 * 3600, // hour
	types.H4:  4 * 3600, // hour
	types.D1:  86400,    // day
	types.W1:  604800,   // week
	types.Mo1: 2592000,  // month (30 days)
}
var INTERVAL_S = map[types.Interval]int{
	types.S1:  1,        // second
	types.S5:  5,        // second
	types.S15: 15,       // second
	types.S30: 30,       // second
	types.M1:  1 * 60,   // minute
	types.M5:  5 * 60,   // minute
	types.M15: 15 * 60,  // minute
	types.M30: 30 * 60,  // minute
	types.H1:  1 * 3600, // hour
	types.H4:  4 * 3600, // hour
	types.D1:  86400,    // day
	types.W1:  604800,   // week
	types.Mo1: 2592000,  // month (30 days)
}
var INTERVAL_MS = map[types.Interval]int64{
	types.S1:  1000,
	types.S5:  5000,
	types.S15: 15000,
	types.S30: 30000,
	types.M1:  60000,
	types.M5:  300000,
	types.M15: 900000,
	types.M30: 1800000,
	types.H1:  3600000,
	types.H4:  14400000,
	types.D1:  86400000,
	types.W1:  604800000,
	types.Mo1: 2592000000,
}
var INTERVAL_CHANNELS = []map[types.Interval]string{
	{ // SPOT
		types.S1:  "spot_one_second_live",
		types.S5:  "spot_five_second_live",
		types.S15: "spot_fifteen_second_live",
		types.S30: "spot_thirty_second_live",
		types.M1:  "spot_one_minute_live",
		types.M5:  "spot_five_minute_live",
		types.M15: "spot_fifteen_minute_live",
		types.M30: "spot_thirty_minute_live",
		types.H1:  "spot_one_hour_live",
		types.H4:  "spot_four_hour_live",
		types.D1:  "spot_one_day_live",
		types.W1:  "spot_one_week_live",
		types.Mo1: "spot_one_month_live",
	},
	{ // FUTURES
		types.S1:  "future_one_second_live",
		types.M1:  "future_one_minute_live",
		types.M5:  "future_five_minute_live",
		types.M15: "future_fifteen_minute_live",
		types.M30: "future_thirty_minute_live",
		types.H1:  "future_one_hour_live",
		types.H4:  "future_four_hour_live",
		types.D1:  "future_one_day_live",
		types.W1:  "future_one_week_live",
		types.Mo1: "future_one_month_live",
	},
	{ // FOREX
		types.S1:  "forex_one_second_live",
		types.S5:  "forex_five_second_live",
		types.S15: "forex_fifteen_second_live",
		types.S30: "forex_thirty_second_live",
		types.M1:  "forex_one_minute_live",
		types.M5:  "forex_five_minute_live",
		types.M15: "forex_fifteen_minute_live",
		types.M30: "forex_thirty_minute_live",
		types.H1:  "forex_one_hour_live",
		types.H4:  "forex_four_hour_live",
		types.D1:  "forex_one_day_live",
		types.W1:  "forex_one_week_live",
		types.Mo1: "forex_one_month_live",
	},
	{ // NOBITEX
		// types.S1:  "nobitex_one_second_live",
		types.M1:  "nobitex_one_minute_live",
		types.M5:  "nobitex_five_minute_live",
		types.M15: "nobitex_fifteen_minute_live",
		types.M30: "nobitex_thirty_minute_live",
		types.H1:  "nobitex_one_hour_live",
		types.H4:  "nobitex_four_hour_live",
		types.D1:  "nobitex_one_day_live",
		// types.W1:  "nobitex_one_week_live",
		// types.Mo1:  "nobitex_one_month_live",
	},
	{ // BITYCLE
		// types.S1:  "nobitex_one_second_live",
		types.M1:  "nobitex_one_minute_live",
		types.M5:  "nobitex_five_minute_live",
		types.M15: "nobitex_fifteen_minute_live",
		types.M30: "nobitex_thirty_minute_live",
		types.H1:  "nobitex_one_hour_live",
		types.H4:  "nobitex_four_hour_live",
		types.D1:  "nobitex_one_day_live",
		types.W1:  "nobitex_one_week_live",
		// types.Mo1:  "nobitex_one_month_live",
	},
}

var TIMEFRAME_INTERVAL_MAP = []map[types.Timeframe]types.Interval{
	{
		"1s":  types.S1,
		"5s":  types.S5,
		"15s": types.S15,
		"30s": types.S30,
		"1m":  types.M1,
		"5m":  types.M5,
		"15m": types.M15,
		"30m": types.M30,
		"1h":  types.H1,
		"4h":  types.H4,
		"1d":  types.D1,
		"1w":  types.W1,
		"1M":  types.Mo1,
	}, // Spot
	{
		"1s":  types.S1,
		"5s":  types.S5,
		"15s": types.S15,
		"30s": types.S30,
		"1m":  types.M1,
		"5m":  types.M5,
		"15m": types.M15,
		"30m": types.M30,
		"1h":  types.H1,
		"4h":  types.H4,
		"1d":  types.D1,
		"1w":  types.W1,
		"1M":  types.Mo1,
	}, // Future
	{
		"1S":  types.S1,
		"5S":  types.S5,
		"15S": types.S15,
		"30S": types.S30,
		"1":   types.M1,
		"5":   types.M5,
		"15":  types.M15,
		"30":  types.M30,
		"60":  types.H1,
		"240": types.H4,
		"D":   types.D1,
		"W":   types.W1,
		"M":   types.Mo1,
	}, // Forex
	{
		"D":   types.D1,
		"60":  types.H1,
		"240": types.H4,
		"1":   types.M1,
		"5":   types.M5,
		"15":  types.M15,
		"30":  types.M30,
	}, // Nobitex
	{
		"1":   types.M1,
		"5":   types.M5,
		"15":  types.M15,
		"30":  types.M30,
		"240": types.H4,
		"60":  types.H1,
		"D":   types.D1,
		// "?":  types.W1,
	}, // Bitycle
}

var INTERVAL_TIMEFRAME_MAP = []map[types.Interval]types.Timeframe{
	{
		types.S1:  "1s",
		types.S5:  "5s",
		types.S15: "15s",
		types.S30: "30s",
		types.M1:  "1m",
		types.M5:  "5m",
		types.M15: "15m",
		types.M30: "30m",
		types.H1:  "1h",
		types.H4:  "4h",
		types.D1:  "1d",
		types.W1:  "1w",
		types.Mo1: "1M",
	}, // Spot
	{
		types.S1:  "1s",
		types.S5:  "5s",
		types.S15: "15s",
		types.S30: "30s",
		types.M1:  "1m",
		types.M5:  "5m",
		types.M15: "15m",
		types.M30: "30m",
		types.H1:  "1h",
		types.H4:  "4h",
		types.D1:  "1d",
		types.W1:  "1w",
		types.Mo1: "1M",
	}, // Future
	{
		types.S1:  "1S",
		types.S5:  "5S",
		types.S15: "15S",
		types.S30: "30S",
		types.M1:  "1",
		types.M5:  "5",
		types.M15: "15",
		types.M30: "30",
		types.H1:  "60",
		types.H4:  "240",
		types.D1:  "D",
		types.W1:  "W",
		types.Mo1: "M",
	}, // Forex
	{
		types.D1:  "D",
		types.H1:  "60",
		types.H4:  "240",
		types.M1:  "1",
		types.M5:  "5",
		types.M15: "15",
		types.M30: "30",
	}, // Nobitex
	{
		types.M1:  "1",
		types.M5:  "5",
		types.M15: "15",
		types.M30: "30",
		types.H4:  "240",
		types.H1:  "60",
		types.D1:  "D",
		// types.W1:  "?",
	}, // Bitycle
}

var LIVE_FIX_LIMITS = []map[types.Interval]int{
	{
		types.S1:  1000,
		types.S5:  200,
		types.S15: 66,
		types.S30: 33,
		types.M1:  15,
		types.M5:  15,
		types.M15: 2,
		types.M30: 2,
		types.H1:  2,
		types.H4:  1,
		types.D1:  1,
		types.W1:  1,
		types.Mo1: 1,
	}, // Spot
	{
		types.M1:  15,
		types.M5:  15,
		types.M15: 2,
		types.M30: 2,
		types.H1:  2,
		types.H4:  1,
		types.D1:  1,
		types.W1:  1,
		types.Mo1: 1,
	}, // Future
	{}, // Forex
	{
		types.M1:  15,
		types.M5:  15,
		types.M15: 2,
		types.M30: 2,
		types.H1:  2,
		types.H4:  1,
		types.D1:  1,
	}, // Nobitex
	{
		types.M1:  15,
		types.M5:  15,
		types.M15: 2,
		types.M30: 2,
		types.H1:  2,
		types.H4:  1,
		types.D1:  1,
		// types.W1:  "?",
	}, // Bitycle
}

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
