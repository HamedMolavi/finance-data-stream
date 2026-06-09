package tradingview

import (
	"encoding/json"
	"fmt"
)

type SendMessage struct {
	M string `json:"m"`
	P []any  `json:"p"`
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// {"m":"series_completed","p":["cs_DcCNfv1DTMH6","sds_1","streaming","s2",{"rt_update_period":0,"data_completed":"limit"}],"t":1767272908,"t_ms":1767272908585}
type RawCompleteMsg []byte
type CompleteMsg struct {
	M    string
	P    CompletePayload
	T    int64 `json:"t"`    // response time
	T_MS int64 `json:"t_ms"` // response time in milli seconds
}
type CompletePayload struct {
	ChartSession       string         // p[0]: "cs_DcCNfv1DTMH6"
	SdsContainerName   string         // p[1]: "sds_1"
	SdsContainerStatus string         // p[2]: "sds_1"
	SeriesName         string         // p[3]: "s3"
	Reason             CompleteReason // p[4]: {"rt_update_period":0,"data_completed":"limit"} OR {"rt_update_period":0}
}
type CompleteReason struct {
	RtUpdatePeriod      *int64  `json:"rt_update_period,omitempty"` // 0
	DataCompletedReason *string `json:"data_completed,omitempty"`   // optional("limit")
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// [
//
//	map[
//		m:timescale_update
//		p:[
//			cs_exuvvlmaljkm
//			map[
//				sds_1: map[
//					lbs: map[bar_close_time:1.755216e+09]
//					node: har1-charts-free-4-series-seiie-1
//					ns: map[d: indexes:[]]
//					s: [
//						map[i:0 v:[1.7550432e+09 9.9993752201e+08 9.9993752201e+08 9.9993752201e+08 9.9993752201e+08]]
//						map[i:1 v:[1.7551296e+09 1.04564625341e+09 1.04564625341e+09 1.04564625341e+09 1.04564625341e+09]]
//					]
//					t:s1
//				]
//			]
//			map[
//				changes:[1.7550432e+09 1.7551296e+09]
//				index:0
//				index_diff:[]
//				marks:[[40 1.7550432e+09 0] [40 1.7551296e+09 1]]
//				zoffset:0
//			]
//		]
//		t:1.76225649e+09
//		t_ms:1.762256490841e+12
//	]
//
// ]
type RawMessage []byte
type Message struct {
	M string  `json:"m"`
	P Payload `json:"p"`
	// T   float64 `json:"t,omitempty"`
	// Tms float64 `json:"t_ms,omitempty"`
}
type Payload struct {
	ChartSessionID ChartSessionID // p[0]
	Sds            SdsContainer   // p[1]
	// Third  MetaChanges  // p[2]
}

type SdsContainer struct {
	Sds1 Sds1 `json:"sds_1"`
}
type Sds1 struct {
	Lbs     Lbs       `json:"lbs"`
	Candles []*Candle `json:"s"`
	// Node string        `json:"node"`
	// Ns NS            `json:"ns"`
	// T string        `json:"t"`
}
type Lbs struct {
	BarCloseTime float64 `json:"bar_close_time"`
}
type NS struct {
	D       string `json:"d,omitempty"`
	Indexes any    `json:"indexes,omitempty"` // shown as []
}
type Candle struct {
	I int       `json:"i"`
	V []float64 `json:"v"` // [1.7551296e+09 1.04564625341e+09 1.04564625341e+09 1.04564625341e+09 1.04564625341e+09]
}
type MetaChanges struct {
	Changes   []float64   `josn:"changes"`
	Index     int         `josn:"index"`
	IndexDiff []int       `josn:"index_diff"`
	Marks     [][]float64 `josn:"marks"`
	Zoffset   float64     `josn:"zoffset"`
}

// UnmarshalJSON handles the heterogenous array for Payload:
func (p *Payload) UnmarshalJSON(data []byte) error {
	// decode into raw messages array
	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return fmt.Errorf("payload is not a JSON array: %w", err)
	}

	// item 0 -> string
	if err := json.Unmarshal(rawItems[0], &p.ChartSessionID); err != nil {
		return fmt.Errorf("failed to unmarshal payload[0] as string: %w", err)
	}

	// item 1 -> SdsContainer
	if err := json.Unmarshal(rawItems[1], &p.Sds); err != nil {
		return fmt.Errorf("failed to unmarshal payload[1] as SdsContainer: %w", err)
	}
	// item 2 -> MetaChanges
	// if err := json.Unmarshal(rawItems[2], &p.Third); err != nil {
	// 	return fmt.Errorf("failed to unmarshal payload[2] as MetaChanges: %w", err)
	// }

	return nil
}

// UnmarshalMessage decodes the outermost array into []Message
func UnmarshalMessage(data RawMessage) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return &msg, fmt.Errorf("unmarshal messages: %w", err)
	}
	return &msg, nil
}

// UnmarshalMessage decodes the outermost array into []Message
func UnmarshalCompMessage(data RawCompleteMsg) (*CompleteMsg, error) {
	var msg CompleteMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return &msg, fmt.Errorf("unmarshal messages: %w", err)
	}
	return &msg, nil
}
