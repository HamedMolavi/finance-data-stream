package manager

import "github.com/HamedMolavi/finance-data-stream/types"

type JobBody struct {
	Symbols     []string // nil means all
	Timeframes  []types.SystemTimeframe
	Limit       int // If you provide Limit, Start and End will be ignored
	Start       string
	End         string
	DeleteFirst bool `json:"delete_first,omitempty"`
}
