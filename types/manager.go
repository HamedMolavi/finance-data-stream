package types

import "time"

// type REQUEST_MODE int64
type REQUEST_TYPE int64

type Emitter interface {
	RowChannel(interval Interval) chan<- *Kline
	Prefetch(symbols map[Symbol]Oldest, interval Interval, from, to time.Time)
	FindGaps(symbol string, interval Interval, from, to time.Time) ([]*GapReport, error)
	FindGapsForSymbols(symbol map[Symbol]Oldest, interval Interval, from, to time.Time) ([]*GapReport, error)
	Delete(symbols map[Symbol]Oldest, interval Interval, from, to time.Time)
	IsBusy() bool
}

type AggTradeResponse struct {
	A int    // Aggregate tradeId
	P string // Price
	Q string // Quantity
	F int    // First tradeId
	L int    // Last tradeId
	T int64  // Timestamp
	M bool   // Was the buyer the maker?
}
