package types

import "time"

type GapReport struct {
	Symbol   string
	Start    time.Time
	End      time.Time
	Interval Interval
}
