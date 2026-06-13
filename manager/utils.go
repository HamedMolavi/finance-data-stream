package manager

import (
	"fmt"
	"strconv"
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
	"github.com/HamedMolavi/finance-data-stream/utils"
)

// Returns start time in millisecond
func ParseRecordTime(rec string) (int64, error) {
	if startTime, err := utils.ToInt64(rec); err != nil {
		return 0, err
	} else {
		switch {
		case startTime > 1e17:
			return startTime / 1000_000, nil
		case startTime > 1e14:
			return startTime / 1000, nil
		case startTime > 1e11:
			return startTime, nil
		case startTime > 1e8:
			return startTime * 1000, nil
		}
	}
	return 0, fmt.Errorf("ParseRecordTime: Couldn't detect start time order")
}

func ParseKlineRecord(symbol string, interval, transformInterval types.Interval, rec []string, kline *types.Kline) (*types.Kline, error) {
	if len(rec) < 11 {
		return nil, fmt.Errorf("not enough columns: got %d", len(rec))
	}
	var err error

	openTime, err := ParseRecordTime(rec[0]) // in millisecond
	if err != nil {
		return nil, err
	}
	closeTime, err := ParseRecordTime(rec[6]) // in millisecond
	if err != nil {
		return nil, err
	}

	open := rec[1]
	high := rec[2]
	low := rec[3]
	close := rec[4]
	volume := rec[5]
	convopen, err := utils.ToFloat64(rec[1])
	if err != nil {
		return nil, fmt.Errorf("col1(Open): %w", err)
	}
	convhigh, err := utils.ToFloat64(rec[2])
	if err != nil {
		return nil, fmt.Errorf("col2(High): %w", err)
	}
	convlow, err := utils.ToFloat64(rec[3])
	if err != nil {
		return nil, fmt.Errorf("col3(Low): %w", err)
	}
	convclose, err := utils.ToFloat64(rec[4])
	if err != nil {
		return nil, fmt.Errorf("col4(Close): %w", err)
	}
	convvolume, err := utils.ToFloat64(rec[5])
	if err != nil {
		return nil, fmt.Errorf("col5(Volume): %w", err)
	}
	transformFlag := interval != transformInterval
	intervalMs := types.INTERVAL_MS[transformInterval]
	if !transformFlag || openTime%intervalMs == 0 {
		newKline := &types.Kline{
			Symbol:     symbol,
			Interval:   transformInterval,
			StartTime:  openTime,
			StartTimeS: openTime / 1000,
			Open:       convopen,
			OpenStr:    open,
			High:       convhigh,
			HighStr:    high,
			Low:        convlow,
			LowStr:     low,
			Close:      convclose,
			CloseStr:   close,
			Volume:     convvolume,
			VolumeStr:  volume,
			CloseTime:  closeTime,
			CloseTimeS: (closeTime / 1000) + 1,
		}
		return newKline, nil
	} else if kline != nil {
		// update the existing candle
		kline.Volume = kline.Volume + convvolume
		kline.VolumeStr = strconv.FormatFloat(kline.Volume, 'f', 5, 64)
		kline.CloseTime = closeTime
		kline.CloseTimeS = (closeTime / 1000) + 1
		kline.Close = convclose
		kline.CloseStr = close
		if kline.High < convhigh {
			kline.High = convhigh
			kline.HighStr = high
		} else if kline.Low > convlow {
			kline.Low = convlow
			kline.LowStr = low
		}
	}
	return nil, nil
}

func ParseTradeRecord(symbol string, interval types.Interval, rec []string, kline *types.Kline) (*types.Kline, error) {
	// 0,					1,				2,					3,							4,							5,							6
	// ID,				Price,		Quantity,		First tradeId,	Last tradeId,		Time (ms),			IsBuyerMaker
	// 18266802,	7240.80,	0.010,			25090490,				25090490,				1577750402288,	false
	if len(rec) < 7 {
		return nil, fmt.Errorf("not enough columns: got %d", len(rec))
	}
	var err error

	tradeTime, err := ParseRecordTime(rec[5]) // in millisecond
	if err != nil {
		return nil, err
	}
	priceStr := rec[1]
	qtyStr := rec[2]
	price, err := utils.ToFloat64(rec[1])
	if err != nil {
		return nil, fmt.Errorf("col1(Price): %w", err)
	}
	qty, err := utils.ToFloat64(rec[2])
	if err != nil {
		return nil, fmt.Errorf("col2(Qty): %w", err)
	}

	intervalMs := types.INTERVAL_MS[interval]
	if kline == nil || kline.CloseTime < tradeTime {
		startMs := utils.TruncateEpochMillis(tradeTime, time.Duration(intervalMs)*time.Millisecond)
		startS := startMs / 1000
		newKline := &types.Kline{
			Symbol:     symbol,
			Interval:   interval,
			StartTime:  startMs,
			StartTimeS: startS,
			Open:       price,
			OpenStr:    priceStr,
			High:       price,
			HighStr:    priceStr,
			Low:        price,
			LowStr:     priceStr,
			Close:      price,
			CloseStr:   priceStr,
			Volume:     qty,
			VolumeStr:  qtyStr,
			CloseTime:  startMs + intervalMs - 1,
			CloseTimeS: startS + intervalMs/1000,
			// Time:       t,
		}

		return newKline, nil
	} else if kline != nil {
		// update the existing candle
		kline.Close = price
		kline.CloseStr = priceStr
		kline.Volume += qty
		kline.VolumeStr = strconv.FormatFloat(kline.Volume, 'f', 5, 64)
		if price < kline.Low {
			kline.Low = price
			kline.LowStr = priceStr
		}
		if price > kline.High {
			kline.High = price
			kline.HighStr = priceStr
		}
	}
	return nil, nil
}
