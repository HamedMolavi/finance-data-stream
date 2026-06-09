package types

type Trade struct {
	// EventType string `json:"e"` // Event type: "aggTrade"
	// EventTime int64  `json:"E"` // event time
	Symbol      string  `json:"s"`
	Time        int64   `json:"T"` // epoch-ms
	Price       float64 `json:"p"`
	PriceStr    string  `json:"-"`
	Quantity    float64 `json:"q"`
	QuantityStr string  `json:"-"`
	// AggregateTradeId int64  `json:"a"`
	// FirstTradeId     int64  `json:"f"`
	// LastTradeId      int64  `json:"l"`
	// other fields omitted
}
