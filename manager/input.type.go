package manager

import (
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
)

type ManagerTime struct {
	T time.Time // corrected time
	F bool      // flag indicates whether the user entered this time or not
}
type ManagerInput struct {
	Id          int64
	Symbols     map[types.Symbol]types.Oldest // nil means all
	Intervals   []types.Interval
	Limit       int // If you provide Limit, Start and End will be ignored
	Start       *ManagerTime
	End         *ManagerTime
	DeleteFirst bool
}
