package utils

import (
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
)

func TruncateEpochMillis(tsMillis int64, duration time.Duration) int64 {
	dMillis := int64(duration / time.Millisecond)
	return (tsMillis / dMillis) * dMillis
}

func TruncateToISOWeek(tsMillis int64) int64 {
	t := time.UnixMilli(tsMillis)
	year, week := t.ISOWeek()
	// ISO week starts on Monday
	firstDay := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	isoWeekStart := firstDay.AddDate(0, 0, (week-1)*7)
	isoWeekStart = isoWeekStart.AddDate(0, 0, -int(isoWeekStart.Weekday()-time.Monday))

	return isoWeekStart.UnixMilli()
}

func TruncateToMonth(tsMillis int64) (int64, int64) {
	t := time.UnixMilli(tsMillis)
	truncated := time.Date(
		t.Year(), t.Month(), 1,
		0, 0, 0, 0,
		time.UTC,
	)
	nextMonth := time.Date(
		t.Year(), t.Month()+1, 1,
		0, 0, 0, 0,
		time.UTC,
	)
	return truncated.UnixMilli(), nextMonth.UnixMilli()
}

func LastStartTime(interval types.Interval, multiplier int) int64 {
	now := time.Now()
	var startTime int64
	switch interval {
	case types.S1:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Second) - int64((time.Duration(multiplier-1) * time.Second / time.Millisecond))
	case types.S5:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(5)*time.Second) - int64((time.Duration(multiplier-1) * time.Second / time.Millisecond))
	case types.S15:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(15)*time.Second) - int64((time.Duration(multiplier-1) * time.Second / time.Millisecond))
	case types.S30:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(30)*time.Second) - int64((time.Duration(multiplier-1) * time.Second / time.Millisecond))
	case types.M1:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Minute) - int64((time.Duration(multiplier-1) * time.Minute / time.Millisecond))
	case types.M5:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(5)*time.Minute) - int64((time.Duration(multiplier-1) * time.Minute / time.Millisecond))
	case types.M15:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(15)*time.Minute) - int64((time.Duration(multiplier-1) * time.Minute / time.Millisecond))
	case types.M30:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(30)*time.Minute) - int64((time.Duration(multiplier-1) * time.Minute / time.Millisecond))
	case types.H1:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Hour) - int64((time.Duration(multiplier-1) * time.Hour / time.Millisecond))
	case types.H4:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(4)*time.Hour) - int64((time.Duration(multiplier-1) * time.Hour / time.Millisecond))
	case types.D1:
		startTime = TruncateEpochMillis(now.UnixMilli(), time.Duration(24)*time.Hour) - int64((time.Duration(multiplier-1) * time.Hour / time.Millisecond))
	case types.W1:
		startTime = TruncateToISOWeek(now.UnixMilli())
	case types.Mo1:
		if multiplier > 12 {
			multiplier = 12
		}
		startTime = time.Date(now.Year(), now.Month()-time.Month(multiplier-1), 1, 0, 0, 0, 0, time.UTC).Unix()
	}
	return startTime / 1000
}
