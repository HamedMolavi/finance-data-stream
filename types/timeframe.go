package types

type Timeframe string
type SystemTimeframe Timeframe

const (
	TFS1  SystemTimeframe = "1s"
	TFS5                  = "5s"
	TFS15                 = "15s"
	TFS30                 = "30s"
	TFM1                  = "1m"
	TFM5                  = "5m"
	TFM15                 = "15m"
	TFM30                 = "30m"
	TFH1                  = "1h"
	TFH4                  = "4h"
	TFD1                  = "1d"
	TFW1                  = "1w"
	TFMo1                 = "1M"
)

var TIMEFRAME_INTERVAL_MAP = []map[Timeframe]Interval{
	{
		"1s":  S1,
		"5s":  S5,
		"15s": S15,
		"30s": S30,
		"1m":  M1,
		"5m":  M5,
		"15m": M15,
		"30m": M30,
		"1h":  H1,
		"4h":  H4,
		"1d":  D1,
		"1w":  W1,
		"1M":  Mo1,
	}, // Spot
	{
		"1s":  S1,
		"5s":  S5,
		"15s": S15,
		"30s": S30,
		"1m":  M1,
		"5m":  M5,
		"15m": M15,
		"30m": M30,
		"1h":  H1,
		"4h":  H4,
		"1d":  D1,
		"1w":  W1,
		"1M":  Mo1,
	}, // Future
	{
		"1S":  S1,
		"5S":  S5,
		"15S": S15,
		"30S": S30,
		"1":   M1,
		"5":   M5,
		"15":  M15,
		"30":  M30,
		"60":  H1,
		"240": H4,
		"D":   D1,
		"W":   W1,
		"M":   Mo1,
	}, // Forex
	{
		"D":   D1,
		"60":  H1,
		"240": H4,
		"1":   M1,
		"5":   M5,
		"15":  M15,
		"30":  M30,
	}, // Nobitex
	{
		"1":   M1,
		"5":   M5,
		"15":  M15,
		"30":  M30,
		"240": H4,
		"60":  H1,
		"D":   D1,
		// "?":  W1,
	}, // Bitycle
}

var INTERVAL_TIMEFRAME_MAP = []map[Interval]Timeframe{
	{
		S1:  "1s",
		S5:  "5s",
		S15: "15s",
		S30: "30s",
		M1:  "1m",
		M5:  "5m",
		M15: "15m",
		M30: "30m",
		H1:  "1h",
		H4:  "4h",
		D1:  "1d",
		W1:  "1w",
		Mo1: "1M",
	}, // Spot
	{
		S1:  "1s",
		S5:  "5s",
		S15: "15s",
		S30: "30s",
		M1:  "1m",
		M5:  "5m",
		M15: "15m",
		M30: "30m",
		H1:  "1h",
		H4:  "4h",
		D1:  "1d",
		W1:  "1w",
		Mo1: "1M",
	}, // Future
	{
		S1:  "1S",
		S5:  "5S",
		S15: "15S",
		S30: "30S",
		M1:  "1",
		M5:  "5",
		M15: "15",
		M30: "30",
		H1:  "60",
		H4:  "240",
		D1:  "D",
		W1:  "W",
		Mo1: "M",
	}, // Forex
	{
		D1:  "D",
		H1:  "60",
		H4:  "240",
		M1:  "1",
		M5:  "5",
		M15: "15",
		M30: "30",
	}, // Nobitex
	{
		M1:  "1",
		M5:  "5",
		M15: "15",
		M30: "30",
		H4:  "240",
		H1:  "60",
		D1:  "D",
		// W1:  "?",
	}, // Bitycle
}
