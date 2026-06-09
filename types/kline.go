package types

type KlineHistory struct {
	Symbol     string   `json:"s,omitempty"`
	Interval   Interval `json:"i,omitempty"`
	StartTime  int64    `json:"t,omitempty"` // Kline start time in milliseconds
	StartTimeS int64    `json:"-"`           // Kline start time in seconds
	Open       string   `json:"o,omitempty"`
	High       string   `json:"h,omitempty"`
	Low        string   `json:"l,omitempty"`
	Close      string   `json:"c,omitempty"`
	Volume     string   `json:"v,omitempty"`
	CloseTime  int64    `json:"T,omitempty"` // Kline close time in milliseconds
	CloseTimeS int64    `json:"-"`           // Kline close time in seconds
	Time       int64    `json:"-"`
	IsClosed   bool     `json:"x,omitempty"`
	Published  bool     `json:"-"`
}

type Kline struct {
	// ===== Ultra-hot update pairs (cache line 0) =====
	// Happens in sym.BuildKlineFromTrade and sym.BuildKlineFromKline
	VolumeStr string  `json:"-"`
	CloseStr  string  `json:"-"`
	Volume    float64 `json:"v,omitempty"`
	Close     float64 `json:"c,omitempty"`
	CloseTime int64   `json:"T,omitempty"`
	// Happens in sym.PublishKline
	Published bool    `json:"-"`
	_         [7]byte // padding to isolate line

	// ===== Ultra-hot conditional pairs (cache line 1) =====
	// Happens in sym.BuildKlineFromTrade and sym.BuildKlineFromKline
	LowStr  string   `json:"-"`
	Low     float64  `json:"l,omitempty"`
	HighStr string   `json:"-"`
	High    float64  `json:"h,omitempty"`
	_       [16]byte // padding to isolate line

	// ===== Hot but less frequent (cache line 2) =====
	// Happens in Subscription loops
	StartTime  int64    `json:"t,omitempty"`
	Time       int64    `json:"-"`
	StartTimeS int64    `json:"-"`
	CloseTimeS int64    `json:"-"`
	Interval   Interval `json:"i,omitempty"`

	// ===== Control & interval state (cache line 3) =====
	Symbol   string  `json:"s,omitempty"`
	OpenStr  string  `json:"-"`
	Open     float64 `json:"o,omitempty"`
	IsClosed bool    `json:"x,omitempty"`
}

type NobitexSocketKline struct {
	T int64   `json:"t"` // "t": 1731852900 seconds
	O float64 `json:"o"` // "o": 6240000001.0,
	H float64 `json:"h"` // "h": 6250000000.0,
	L float64 `json:"l"` // "l": 6238000000.0,
	C float64 `json:"c"` // "c": 6238031033.0,
	V float64 `json:"v"` // "v": 1.26
}
type NobitexApiKline struct {
	Status string    `json:"s"`
	T      []int64   `json:"t"`
	O      []float64 `json:"o"`
	H      []float64 `json:"h"`
	L      []float64 `json:"l"`
	C      []float64 `json:"c"`
	V      []float64 `json:"v"`
}

type FarazApiKline struct {
	Result struct {
		O []float64 `json:"o"`
		H []float64 `json:"h"`
		L []float64 `json:"l"`
		C []float64 `json:"c"`
		V []float64 `json:"v"`
		T []int64   `json:"t"`
	}
}
