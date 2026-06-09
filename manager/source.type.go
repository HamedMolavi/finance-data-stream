package manager

import (
	"context"
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
)

type ManagerSourceResult struct {
	ManagerSourceRequest *ManagerSourceRequest
	Count                int64
	Retried              int64
	Errs                 []error
}

type ManagerSourceRequest struct {
	ManagerInput    *ManagerInput
	SpotRequests    []*SpotSourceRequest
	FutureRequests  []*FutureSourceRequest
	NobitexRequests []*NobitexSourceRequest
	ForexRequests   []*ForexSourceRequest
}

type SpotSourceRequest struct {
	Symbol                    string
	Interval                  types.Interval
	TransformInterval         types.Interval
	Intervals                 []types.Interval // Forex or Spot-s or Future-s
	Limit, StartTime, EndTime int
	Weight                    uint64
	// Mode                      REQUEST_MODE
	// Type REQUEST_TYPE
	// URL string
	////////////// Caution: the following fields are only used in Trade and Transform mode
	Start, End time.Time
}

type FutureSourceRequest struct {
	Symbol                    string
	Interval                  types.Interval
	TransformInterval         types.Interval
	Intervals                 []types.Interval // Forex or Spot-s or Future-s
	Limit, StartTime, EndTime int
	Weight                    uint64
	// Mode                      REQUEST_MODE
	// Type REQUEST_TYPE
	// URL string
	////////////// Caution: the following fields are only used in Trade and Transform mode
	Start, End time.Time
}

type NobitexSourceRequest struct {
	Symbol                    string
	Interval                  types.Interval
	Limit, StartTime, EndTime int
	Weight                    uint64
	// Mode                      REQUEST_MODE
	// Type REQUEST_TYPE
	// URL string
	////////////// Caution: the following fields are only used in Trade and Transform mode
	Start, End time.Time
}

type ForexSourceRequest struct {
	Symbols    []types.Symbol
	Intervals  []types.Interval
	Limit      int
	Weight     uint64
	Start, End time.Time
	// Mode                      REQUEST_MODE
	// Type REQUEST_TYPE
	// URL string
}

type sourceResult struct {
	Count   int64
	Retried int64
	Err     error
}
type job[R ForexSourceRequest | NobitexSourceRequest | FutureSourceRequest | SpotSourceRequest] struct {
	ctx      context.Context
	request  *R
	resultCh chan *sourceResult
}
