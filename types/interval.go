package types

type Interval uint8

func (i Interval) String() string {
	return string(SYSTEM_INTERVAL_TIMEFRAME_MAP[i])
}

const (
	S1 Interval = iota
	S5
	S15
	S30
	M1
	M5
	M15
	M30
	H1
	H4
	D1
	W1
	Mo1
	NoInterval
)

var INTERVALS = []Interval{
	S1, S5, S15, S30, // seconds
	M1, M5, M15, M30, // minutes
	H1, H4, // hours
	D1,  // days
	W1,  // weeks
	Mo1, // months
}
var SYSTEM_INTERVAL_TIMEFRAME_MAP = map[Interval]SystemTimeframe{
	S1:  TFS1,
	S5:  TFS5,
	S15: TFS15,
	S30: TFS30,
	M1:  TFM1,
	M5:  TFM5,
	M15: TFM15,
	M30: TFM30,
	H1:  TFH1,
	H4:  TFH4,
	D1:  TFD1,
	W1:  TFW1,
	Mo1: TFMo1,
}
var SYSTEM_TIMEFRAME_INTERVAL_MAP = map[SystemTimeframe]Interval{
	TFS1:  S1,
	TFS5:  S5,
	TFS15: S15,
	TFS30: S30,
	TFM1:  M1,
	TFM5:  M5,
	TFM15: M15,
	TFM30: M30,
	TFH1:  H1,
	TFH4:  H4,
	TFD1:  D1,
	TFW1:  W1,
	TFMo1: Mo1,
}

var INTERVAL_S_64 = map[Interval]int64{
	S1:  1,        // second
	S5:  5,        // second
	S15: 15,       // second
	S30: 30,       // second
	M1:  1 * 60,   // minute
	M5:  5 * 60,   // minute
	M15: 15 * 60,  // minute
	M30: 30 * 60,  // minute
	H1:  1 * 3600, // hour
	H4:  4 * 3600, // hour
	D1:  86400,    // day
	W1:  604800,   // week
	Mo1: 2592000,  // month (30 days)
}
var INTERVAL_S = map[Interval]int{
	S1:  1,        // second
	S5:  5,        // second
	S15: 15,       // second
	S30: 30,       // second
	M1:  1 * 60,   // minute
	M5:  5 * 60,   // minute
	M15: 15 * 60,  // minute
	M30: 30 * 60,  // minute
	H1:  1 * 3600, // hour
	H4:  4 * 3600, // hour
	D1:  86400,    // day
	W1:  604800,   // week
	Mo1: 2592000,  // month (30 days)
}
var INTERVAL_MS = map[Interval]int64{
	S1:  1000,
	S5:  5000,
	S15: 15000,
	S30: 30000,
	M1:  60000,
	M5:  300000,
	M15: 900000,
	M30: 1800000,
	H1:  3600000,
	H4:  14400000,
	D1:  86400000,
	W1:  604800000,
	Mo1: 2592000000,
}
