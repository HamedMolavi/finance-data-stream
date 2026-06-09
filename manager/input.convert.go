package manager

import (
	"maps"
	"slices"
	"time"

	"github.com/HamedMolavi/finance-data-stream/config"
	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/HamedMolavi/finance-data-stream/types"
)

func findLimit(interval types.Interval, start, end time.Time) int {
	return int(int(end.Unix()-start.Unix())/types.INTERVAL_S[interval]) + 1
}

var InputConversionStage = pipeline.TryMapStageFactory(
	func(in *ManagerInput) (out *ManagerSourceRequest, err error) {
		if config.C.Market() == types.FOREX_MARKET {
			out.ForexRequests, err = ForexRequestGenerator(in)
		}
		// else if config.C.Market() == types.NOBITEX_MARKET || config.C.Market() == types.BITYCLE_MARKET {
		// 	out.NobitexRequests, err = NobitexRequestGenerator(in)
		// } else if config.C.Market() == types.FUTURES_MARKET {
		// 	out.FutureRequests, err = FutureRequestGenerator(in)
		// }
		// out.SpotRequests, err = SpotRequestGenerator(in)
		return out, err
	},
)

func ForexRequestGenerator(in *ManagerInput) ([]*ForexSourceRequest, error) {
	out := make([]*ForexSourceRequest, 0)
	symbolsMap, intervals, limit := in.Symbols, in.Intervals, in.Limit

	step := config.C.MaxSymbolPerConnection()
	total := len(symbolsMap)

	symbols := slices.Collect(maps.Keys(symbolsMap))
	//////////////
	for i := 0; i < total; i += step {
		j := min(total, i+step)
		if limit == 0 {
			//////////////
			// In case user didn't enter a limit, we have to find the limit since the forex market is limit based only;
			// we ignore end time in limit calculation
			// and we take start time from user input (this goes back to default start if start is not provided by user)
			// later we use original user input start and end time (or their default values) to filter out unexpected retrieved candles
			//////////////
			start, end := in.Start.T, time.Now()
			for _, interval := range intervals {
				limit = findLimit(interval, start, end)
				out = append(out, &ForexSourceRequest{
					Symbols:   symbols[i:j],
					Intervals: []types.Interval{interval},
					Limit:     limit,
					Weight:    1,
					Start:     in.Start.T, End: in.End.T,
				})
			}
		} else {
			//////////////
			// If user entered limit, we just use that!
			// later we use original user input start and end time (or their default values) to filter out unexpected retrieved candles
			//////////////
			out = append(out, &ForexSourceRequest{
				Symbols:   symbols[i:j],
				Intervals: intervals,
				Limit:     limit,
				Weight:    1,
				Start:     in.Start.T, End: in.End.T,
			})
		}
	}
	return out, nil
}

//////////////////////////////////////////////////////////////////////////
/*

func NobitexRequestGenerator(in *ManagerInput) ([]*types.NobitexSourceRequest, error) {
	out := make([]*types.NobitexSourceRequest, 0)
	// Have in mind the API layout => ?symbol=BTCIRT & resolution=1 & from=1562058167 & to=1763128741 & countback=1 & page=2
	//		if countback is more than 500, we have to paginage
	//		to is mandatory
	var weight uint64 = 2
	mode := NOBITEX_API_REQUEST
	urls := NOBITEX_API_URLS
	symbols, interval := in.Symbols, in.Interval
	timeFrame, ok := utils.INTERVAL_TIMEFRAME_MAP[utils.NOBITEX_MARKET][interval]
	if !ok {
		return out, fmt.Errorf("Unsupported interval (%s) input for nobitex market", timeFrame)
	}

	for symbol := range symbols {
		limit := in.Limit
		if limit == 0 {
			oldest := config.C.SymbolCache().Symbols[types.Symbol(symbol)]
			start, end := utils.InputToTimeFromIn(in)
			if oldest != "" {
				if oldestTime, err := time.Parse(time.RFC3339, string(oldest)); err == nil {
					if start.Before(oldestTime) {
						start = oldestTime
					}
				} else {
					logrus.Warnln("error parsing oldest", oldest, "for", symbol)
				}

			}
			limit = findLimit(interval, start, end)
		}
		if limit > 0 && limit <= 500 {
			// no need to paginate
			if in.EndDate != "" && in.EndTime != "" {
				endStr := fmt.Sprintf("%s %s", in.EndDate, in.EndTime)
				end, _ := time.Parse(utils.DATE_TIME_LAYOUT, endStr)
				out = append(out, &types.NobitexSourceRequest{
					Symbol: string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: mode,
					URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&resolution=%s&countback=%d&to=%d", string(symbol), timeFrame, limit, end.Unix()),
				})
			} else {
				out = append(out, &types.NobitexSourceRequest{
					Symbol: string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: mode,
					URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&resolution=%s&countback=%d&to=%d", string(symbol), timeFrame, limit, time.Now().Unix()),
				})
			}
		} else if limit > 500 {
			// paginate for each 500 candles but keep the coutback as the original
			var to int64
			if in.EndDate != "" && in.EndTime != "" {
				endStr := fmt.Sprintf("%s %s", in.EndDate, in.EndTime)
				end, err := time.Parse(utils.DATE_TIME_LAYOUT, endStr)
				if err != nil {
					to = time.Now().Unix()
				} else {
					to = end.Unix()
				}
			} else {
				to = time.Now().Unix()
			}
			page, j := 1, int(limit/500)+1
			for ; page <= j; page++ {
				out = append(out, &types.NobitexSourceRequest{
					Symbol: string(symbol), Interval: interval, Limit: 500, Weight: weight,
					Type: mode,
					URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&resolution=%s&countback=%d&page=%d&to=%d", string(symbol), timeFrame, limit, page, to),
				})
			}
		}
	}
	return out, nil
}
func FutureSecondRequestGenerator(in *ManagerInput) ([]*types.FutureSourceRequest, error) {
	out := make([]*types.FutureSourceRequest, 0)
	/////////////////////////////////////////////////////////
	symbols, intervals, limit := in.Symbols, in.Intervals, in.Limit
	if len(in.Intervals) == 0 {
		intervals = []types.Interval{in.Interval}
	}

	publicTradeUrl := FUTURES_TRADE_PUBLIC_URL
	var weight uint64 = 20

	for symbol, oldest := range symbols {
		correctedSymbol := strings.ReplaceAll(string(symbol), "_FUTURES", "")
		start, end := utils.InputToTimeFromIn(in)

		if limit != 0 {
			// User entered limit explicitly.
			// Since taking the limit into the action makes a different start time for each interval, we should produce different requests for each interval.
			for _, interval := range intervals {
				// If user has entered end time, we use it. If not, we use now as end time. This is implemented by the utils.InputToTimeFromIn function.
				if in.StartDate == "" || in.StartTime == "" {
					// Since we need to consider limit, but the API only allows us to enter end time, we calculate the start time based on the end time and the limit and ignore the start time entered by user.
					startMilli := end.UnixMilli() - int64(limit)*utils.INTERVAL_MS[interval]
					start = time.UnixMilli(startMilli)
				}
				// If user has entered all of start time, end time and limit, limit parameter is going to be ignored since we have end time and start time defined.
				// Avoid using this mode
				out = append(out, &types.FutureSourceRequest{
					Intervals: intervals,
					Symbol:    string(symbol), Limit: limit, Weight: weight,
					Type:  BINANCE_TRADE_API_REQUEST,
					URL:   FUTURES_TRADE_API_URL,
					Start: start, End: end,
				})
			}
			return out, nil
		}

		// User didn't enter limit, so we go with the start time and the end time
		now := time.Now()
		// Move start forward to 23:59:59 of that day
		startMidnight := time.Date(start.Year(), start.Month(), start.Day(), 23, 59, 59, 1e9-1, time.UTC)
		if startMidnight.After(end) {
			startMidnight = end
		}
		// Move end backward to 00:00:00 of that day
		midnightEnd := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
		if midnightEnd.Before(start) {
			midnightEnd = start
		}
		///////////////////////
		monthArray, didWeMoveMonth := utils.MonthArrayWithoutCount(start, end, correctedSymbol, string(oldest))
		for _, month := range monthArray {
			out = append(out, &types.FutureSourceRequest{
				Intervals: intervals,
				Symbol:    string(symbol), Limit: 0,
				Type: BINANCE_DOWNLOAD_TRADE_REQUEST,
				URL:  fmt.Sprintf(publicTradeUrl, "monthly", correctedSymbol, correctedSymbol, month),
			})
		}
		if didWeMoveMonth && (start.Year() != now.Year() || start.Month() != now.Month()) {
			// if start is not this month
			// the MonthArray already included the whole start month as a hueristic way, so
			// move start time to first day of next month
			start = time.Date(start.Year(), start.Month()+time.Month(len(monthArray)), 1, 0, 0, 0, 0, time.UTC)
		}
		dailyArray := utils.DayArrayWithoutCount(start, end)
		for _, day := range dailyArray {
			out = append(out, &types.FutureSourceRequest{
				Intervals: intervals,
				Symbol:    string(symbol), Limit: 0,
				Type: BINANCE_DOWNLOAD_TRADE_REQUEST,
				URL:  fmt.Sprintf(publicTradeUrl, "daily", correctedSymbol, correctedSymbol, day),
			})
		}
		///////////////////////
		if start.Year() != now.Year() || start.Month() != now.Month() || start.Day() != now.Day() {
			// if start is not today
			// the DayArray already included the whole start day as a hueristic way, so
			// move start time to next day
			start = time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, time.UTC)
		}
		if end.Year() != now.Year() || end.Month() != now.Month() || end.Day() != now.Day() {
			// if end is not today
			// the DayArray already included the whole end day as a hueristic way, so
			// move end time to previous day
			end = time.Date(end.Year(), end.Month(), end.Day()-1, 0, 0, 0, 0, time.UTC)
			// fmt.Println("End is today, new end:", end)
		}
		// fmt.Println("Start from midnight:")
		if startMidnight.After(start) {
			out = append(out, &types.FutureSourceRequest{
				Intervals: intervals,
				Symbol:    string(symbol), Limit: 0, Weight: weight,
				Type:  BINANCE_TRADE_API_REQUEST,
				URL:   FUTURES_TRADE_API_URL,
				Start: start, End: startMidnight,
			})
		}
		if midnightEnd.After(startMidnight) {
			// fmt.Println("End to midnight:")
			out = append(out, &types.FutureSourceRequest{
				Intervals: intervals,
				Symbol:    string(symbol), Limit: 0, Weight: weight,
				Type:  BINANCE_TRADE_API_REQUEST,
				URL:   FUTURES_TRADE_API_URL,
				Start: midnightEnd, End: end,
			})
		}

	}
	return out, nil

}
func FutureMinuteRequestGenerator(in *ManagerInput) ([]*types.FutureSourceRequest, error) {
	out := make([]*types.FutureSourceRequest, 0)
	/////////////////////////////////////////////////////////
	symbols, interval, limit := in.Symbols, in.Interval, in.Limit
	tf := utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval]

	var userEnteredLimit bool
	if limit == 0 {
		start, end := utils.InputToTimeFromIn(in)
		limit = findLimit(interval, start, end)
	} else {
		userEnteredLimit = true
	}
	publicUrl := FUTURES_KLINE_PUBLIC_URL
	apiUrl := FUTURES_API_URLS[0]
	var weight uint64
	switch {
	case limit > 0 && limit < 100:
		weight = 1
	case limit >= 100 && limit < 500:
		weight = 2
	case limit >= 500 && limit <= 1000:
		weight = 5
	case limit > 1000:
		weight = 10
	}
	for symbol, oldest := range symbols {
		correctedSymbol := strings.ReplaceAll(string(symbol), "_FUTURES", "")
		if limit > 0 && limit <= 1000 {
			if in.EndDate != "" && in.EndTime != "" {
				endStr := fmt.Sprintf("%s %s", in.EndDate, in.EndTime)
				end, _ := time.Parse(utils.DATE_TIME_LAYOUT, endStr)
				out = append(out, &types.FutureSourceRequest{
					TransformInterval: interval,
					Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: BINANCE_KLINE_API_REQUEST,
					URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&limit=%d&endTime=%d", correctedSymbol, tf, limit, end.UnixMilli()),
				})
			} else {
				out = append(out, &types.FutureSourceRequest{
					TransformInterval: interval,
					Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: BINANCE_KLINE_API_REQUEST,
					URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&limit=%d", correctedSymbol, tf, limit),
				})
			}
		} else if limit > 1000 {
			start, end := utils.InputToTimeFromIn(in)
			if userEnteredLimit && (in.EndDate == "" || in.EndTime == "") {
				// User entered limit explicitly and NO end time.
				// We don't care for start time for now
				// Since users enter this mode with small limits, we only generate query (no monthly and no daily)
				endMilli := utils.TruncateEpochMillis(time.Now().UnixMilli(), time.Duration(utils.INTERVAL_MS[interval])*time.Millisecond)
				startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
				for _, query := range utils.ApiArrayMilli(startMilli, endMilli, interval) {
					out = append(out, &types.FutureSourceRequest{
						TransformInterval: interval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&%s", correctedSymbol, tf, query.Q),
					})
				}
				return out, nil
			} else if userEnteredLimit && in.EndDate != "" && in.EndTime != "" {
				// User entered limit and end time explicitly.
				// We don't care for start time for now
				// Since users enter this mode with small limits, we only generate query (no monthly and no daily)
				endMilli := end.UnixMilli()
				startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
				for _, query := range utils.ApiArrayMilli(startMilli, endMilli, interval) {
					out = append(out, &types.FutureSourceRequest{
						TransformInterval: interval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&%s", correctedSymbol, tf, query.Q),
					})
				}
				return out, nil
			}
			// User didn't enter limit, so we go with the start time and the end time
			now := time.Now()
			// Move start forward to 23:59:59 of that day
			startMidnight := time.Date(start.Year(), start.Month(), start.Day(), 23, 59, 59, 1e9-1, time.UTC)
			if startMidnight.After(end) {
				startMidnight = end
			}
			// Move end backward to 00:00:00 of that day
			midnightEnd := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
			if midnightEnd.Before(start) {
				midnightEnd = start
			}
			///////////////////////
			monthArray, didWeMoveMonth := utils.MonthArray(start, end, interval, correctedSymbol, string(oldest))
			for _, month := range monthArray {
				out = append(out, &types.FutureSourceRequest{
					TransformInterval: interval, Interval: interval,
					Symbol: string(symbol), Limit: month.Count,
					Type: BINANCE_DOWNLOAD_KLINE_REQUEST,
					URL:  fmt.Sprintf(publicUrl, "monthly", correctedSymbol, tf, correctedSymbol, tf, month.Q),
				})
			}
			if didWeMoveMonth && (start.Year() != now.Year() || start.Month() != now.Month()) {
				// if start is not this month
				// the MonthArray already included the whole start month as a hueristic way, so
				// move start time to first day of next month
				start = time.Date(start.Year(), start.Month()+time.Month(len(monthArray)), 1, 0, 0, 0, 0, time.UTC)
			}
			dailyArray := utils.DayArray(start, end, interval)
			for _, day := range dailyArray {
				out = append(out, &types.FutureSourceRequest{
					TransformInterval: interval, Interval: interval,
					Symbol: string(symbol), Limit: day.Count,
					Type: BINANCE_DOWNLOAD_KLINE_REQUEST,
					URL:  fmt.Sprintf(publicUrl, "daily", correctedSymbol, tf, correctedSymbol, tf, day.Q),
				})
			}
			///////////////////////
			if start.Year() != now.Year() || start.Month() != now.Month() || start.Day() != now.Day() {
				// if start is not today
				// the DayArray already included the whole start day as a hueristic way, so
				// move start time to next day
				start = time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, time.UTC)
			}
			if end.Year() != now.Year() || end.Month() != now.Month() || end.Day() != now.Day() {
				// if end is not today
				// the DayArray already included the whole end day as a hueristic way, so
				// move end time to previous day
				end = time.Date(end.Year(), end.Month(), end.Day()-1, 0, 0, 0, 0, time.UTC)
				// fmt.Println("End is today, new end:", end)
			}
			if !slices.Contains([]types.Interval{types.W1, types.Mo1}, interval) {
				// fmt.Println("Start from midnight:")
				for _, query := range utils.ApiArray(start, startMidnight, interval) {
					// fmt.Println(query)
					out = append(out, &types.FutureSourceRequest{
						TransformInterval: interval, Interval: interval,
						Symbol: string(symbol), Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&%s", correctedSymbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
					})
				}
				if midnightEnd.After(startMidnight) {
					// fmt.Println("End to midnight:")
					for _, query := range utils.ApiArray(midnightEnd, end, interval) {
						// fmt.Println(query)
						out = append(out, &types.FutureSourceRequest{
							TransformInterval: interval, Interval: interval,
							Symbol: string(symbol), Limit: query.Count, Weight: 2,
							Type: BINANCE_KLINE_API_REQUEST,
							URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&%s", correctedSymbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
						})
					}
				}
			}
		}
	}
	return out, nil

}
func FutureRequestGenerator(in *ManagerInput) ([]*types.FutureSourceRequest, error) {
	if slices.Contains([]types.Interval{types.S1, types.S5, types.S15, types.S30}, in.Interval) {
		return FutureSecondRequestGenerator(in)
	} else {
		return FutureMinuteRequestGenerator(in)
	}
}
func SpotTransformRequestGeneratorV2(in *ManagerInput) ([]*types.SpotSourceRequest, error) {
	out := make([]*types.SpotSourceRequest, 0)
	/////////////////////////////////////////////////////////
	// The interval is the intended interval, for example 5s. **We may swap this in next few lines of code**.
	symbols, limit := in.Symbols, in.Limit
	transformIntervals := in.Intervals
	if len(transformIntervals) == 0 {
		transformIntervals = []types.Interval{in.Interval}
	}
	// Since in some markets like spot we can't request 5s interval, we set two variables for it
	// **transformInterval**	 (5s) which is now the intended interval
	// **interval**						 (1s) which is the time interval we can get in that market
	interval := types.S1
	tf := utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval]
	// end time is mandatory and filled with default to Now if user didn't enter it.

	var weight uint64 = 2
	urls := SPOT_API_URLS
	publicUrl := SPOT_KLINE_PUBLIC_URL

	for sym, oldest := range symbols {
		start, end := utils.InputToTimeFromIn(in)
		symbol := string(sym)
		if limit != 0 {
			// user has entered limit, we assume end time is within the current day (mostly now)

			// limit counts for 1s interval here
			// so if you put limit=1000 with intervals=[1s, 5s, 15s, 30s] you are going to get 1000 klines for 1s, 200 klines for 5s, 66 klines for 15s and 33 klines for 30s
			// TODO: with flag to get limit for each interval
			startMilli := end.UnixMilli() - int64(limit)*utils.INTERVAL_MS[interval]
			start = time.UnixMilli(startMilli).UTC()
			out = append(out, &types.SpotSourceRequest{
				Intervals: transformIntervals, Interval: interval,
				Symbol: symbol, Limit: limit, Weight: weight,
				Type:  SPOT_API_WITH_TRANSFORM_REQUEST,
				Start: start, End: end,
				URL: urls[rand.IntN(len(urls))],
				// URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&limit=%d&endTime=%d", symbol, tf, limit, end.UnixMilli()),
			})
			continue
		}
		// user didn't enter limit, the request is either pointing to today or history.

		// *********
		// THIS PART: historical data targeted to binance vision collection
		// *********

		now := time.Now()
		// startMidnight is 23:59:59 of `start time`
		startMidnight := time.Date(start.Year(), start.Month(), start.Day(), 23, 59, 59, 1e9-1, time.UTC)
		// if startMidnight.After(end) {
		// 	startMidnight = end
		// }
		// midnightEnd is 00:00:00 of `end time`
		midnightEnd := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
		if midnightEnd.Before(start) {
			midnightEnd = start
		}
		///////////////////////
		monthArray, didWeMoveMonth := utils.MonthArray(start, end, interval, symbol, string(oldest))
		for _, month := range monthArray {
			out = append(out, &types.SpotSourceRequest{
				Intervals: transformIntervals, Interval: interval,
				Symbol: symbol, Limit: month.Count,
				Type: SPOT_DOWNLOAD_WITH_TRANSFORM_REQUEST,
				URL:  fmt.Sprintf(publicUrl, "monthly", symbol, tf, symbol, tf, month.Q),
			})
		}
		if didWeMoveMonth && (start.Year() != now.Year() || start.Month() != now.Month()) {
			// if start is not this month
			// the MonthArray already included the whole start month as a hueristic way, so
			// move start time to first day of next month
			start = time.Date(start.Year(), start.Month()+time.Month(len(monthArray)), 1, 0, 0, 0, 0, time.UTC)
		}
		dailyArray := utils.DayArray(start, end, interval)
		for _, day := range dailyArray {
			out = append(out, &types.SpotSourceRequest{
				Intervals: transformIntervals, Interval: interval,
				Symbol: symbol, Limit: day.Count,
				Type: SPOT_DOWNLOAD_WITH_TRANSFORM_REQUEST,
				URL:  fmt.Sprintf(publicUrl, "daily", symbol, tf, symbol, tf, day.Q),
			})
		}
		///////////////////////
		if start.Year() != now.Year() || start.Month() != now.Month() || start.Day() != now.Day() {
			// if start is not today
			// the DayArray already included the whole start day as a hueristic way, so
			// move start time to next day
			start = time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, time.UTC)
		}
		if end.Year() != now.Year() || end.Month() != now.Month() || end.Day() != now.Day() {
			// if end is not today
			// the DayArray already included the whole end day as a hueristic way, so
			// move end time to previous day
			end = time.Date(end.Year(), end.Month(), end.Day()-1, 0, 0, 0, 0, time.UTC)
			// fmt.Println("End is today, new end:", end)
		}

		// *********
		// THIS PART: little chunks of data targeted to binance API
		// *********

		if startMidnight.After(start) && !startMidnight.After(end) {
			// fmt.Println("Start from midnight:", startMidnight, start)
			out = append(out, &types.SpotSourceRequest{
				Intervals: transformIntervals, Interval: interval,
				Symbol: symbol, Limit: 0, Weight: weight,
				Start: start, End: startMidnight,
				Type: SPOT_API_WITH_TRANSFORM_REQUEST,
				// URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
				URL: urls[rand.IntN(len(urls))],
			})
		}
		if end.After(midnightEnd) {
			// fmt.Println("End to midnight:", midnightEnd, end)
			out = append(out, &types.SpotSourceRequest{
				Intervals: transformIntervals, Interval: interval,
				Symbol: symbol, Limit: 0, Weight: weight,
				Start: midnightEnd, End: end,
				Type: SPOT_API_WITH_TRANSFORM_REQUEST,
				// URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
				URL: urls[rand.IntN(len(urls))],
			})
		}

	}
	return out, nil
}
func SpotTransformRequestGenerator(in *ManagerInput) ([]*types.SpotSourceRequest, error) {
	out := make([]*types.SpotSourceRequest, 0)
	/////////////////////////////////////////////////////////
	// The interval is the intended interval, for example 5s. **We may swap this in next few lines of code**.
	symbols, interval, limit := in.Symbols, in.Interval, in.Limit
	// Since in some markets like spot we can't request 5s interval, we set two variables for it
	// **transformInterval**	 (5s) which is now the intended interval
	// **interval**						 (1s) which is the time interval we can get in that market
	transformInterval := interval
	interval = types.S1
	var userEnteredLimit bool
	if limit == 0 {
		// Here we calculate the limit based on the base interval that later may be transformed into another interval or not.
		// For exmpale user entered 5s interval with a start and end time which we need 500 candles of 5s to compelete.
		// But we actually should have 2500 1s candles to be transformed into 500 5s candles, so limit would be 2500
		start, end := utils.InputToTimeFromIn(in)
		limit = findLimit(interval, start, end)
		// TODO P1: the problem here is that at the edge of each 1000th candle, we may not be able to form a transformed candle.
		//					Assume we have 1100 limit and the 1000th candle which falls into the first batch has start time of 1767253332 seconds.
		//					We can not create a candle based on since the open price would be inaccurate.
		//					Also, the 1001th candle would be the last candle in the second batch with start time of 1767253331. Here too we can not end the transformation.

	} else {
		// The limit here is entered for the intended interval, for example 5s. So we need to compute the limit for interval
		limit = limit * utils.INTERVAL_S[transformInterval] / utils.INTERVAL_S[interval]
		userEnteredLimit = true
	}
	var weight uint64 = 2
	urls := SPOT_API_URLS
	publicUrl := SPOT_KLINE_PUBLIC_URL

	tf := utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval]
	for symbol, oldest := range symbols {
		correctedSymbol := strings.ReplaceAll(string(symbol), "_FUTURES", "")
		if limit > 0 && limit <= 1000 {
			// Regarding to P1 since limit is less than 1000 what we can do here depends on whether end time is specified or not:
			if in.EndDate != "" && in.EndTime != "" {
				endStr := fmt.Sprintf("%s %s", in.EndDate, in.EndTime)
				end, _ := time.Parse(utils.DATE_TIME_LAYOUT, endStr)
				// Regarding to P1 since limit is less than 1000 and end time is specified:
				//		we only may encounter that error at the first of the only batch,
				// 		so we move the portion of klines backward a little to include that first kline too.
				end = time.UnixMilli(utils.TruncateEpochMillis(end.UnixMilli(), time.Duration(utils.INTERVAL_MS[transformInterval])*time.Millisecond))
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: transformInterval,
					Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: BINANCE_KLINE_API_REQUEST,
					URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&limit=%d&endTime=%d", correctedSymbol, tf, limit, end.UnixMilli()),
				})
			} else {
				// Regarding to P1 since limit is less than 1000 and end time is NOT specified:
				//		We could add some klines to include the first transformation candle, but only if it does not make the limit more than 1000
				//		TODO: we have a problem here if the amount of added klines would cause the limit to be more than 1000.
				var aFewKlinesMore int = (limit/1000 + 1) * utils.INTERVAL_S[transformInterval] / utils.INTERVAL_S[interval]
				if limit+aFewKlinesMore <= 1000 {
					limit += aFewKlinesMore
				}
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: transformInterval,
					Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: BINANCE_KLINE_API_REQUEST,
					URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&limit=%d", correctedSymbol, tf, limit),
				})
			}
		} else if limit > 1000 {
			start, end := utils.InputToTimeFromIn(in)
			if userEnteredLimit && (in.EndDate == "" || in.EndTime == "") {
				// User entered limit explicitly and NO end time.
				// We don't care for start time for now
				// Since users enter this mode with small limits, we only generate query (no monthly and no daily)
				endMilli := utils.TruncateEpochMillis(time.Now().UnixMilli(), time.Duration(utils.INTERVAL_MS[transformInterval])*time.Millisecond)
				startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
				for _, query := range utils.ApiArrayMilli(startMilli, endMilli, interval) {
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: transformInterval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", correctedSymbol, tf, query.Q),
					})
				}
				return out, nil
			} else if userEnteredLimit && in.EndDate != "" && in.EndTime != "" {
				// User entered limit and end time explicitly.
				// We don't care for start time for now
				// Since users enter this mode with small limits, we only generate query (no monthly and no daily)
				endMilli := end.UnixMilli()
				startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
				for _, query := range utils.ApiArrayMilli(startMilli, endMilli, interval) {
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: transformInterval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", correctedSymbol, tf, query.Q),
					})
				}
				return out, nil
			}
			// User didn't enter limit, so we go with the start time and the end time
			now := time.Now()
			// Move start forward to 23:59:59 of that day
			startMidnight := time.Date(start.Year(), start.Month(), start.Day(), 23, 59, 59, 1e9-1, time.UTC)
			if startMidnight.After(end) {
				startMidnight = end
			}
			// Move end backward to 00:00:00 of that day
			midnightEnd := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
			if midnightEnd.Before(start) {
				midnightEnd = start
			}
			///////////////////////
			monthArray, didWeMoveMonth := utils.MonthArray(start, end, interval, correctedSymbol, string(oldest))
			for _, month := range monthArray {
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: transformInterval, Interval: interval,
					Symbol: string(symbol), Limit: month.Count,
					Type: BINANCE_DOWNLOAD_KLINE_REQUEST,
					URL:  fmt.Sprintf(publicUrl, "monthly", correctedSymbol, tf, correctedSymbol, tf, month.Q),
				})
			}
			if didWeMoveMonth && (start.Year() != now.Year() || start.Month() != now.Month()) {
				// if start is not this month
				// the MonthArray already included the whole start month as a hueristic way, so
				// move start time to first day of next month
				start = time.Date(start.Year(), start.Month()+time.Month(len(monthArray)), 1, 0, 0, 0, 0, time.UTC)
			}
			dailyArray := utils.DayArray(start, end, interval)
			for _, day := range dailyArray {
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: transformInterval, Interval: interval,
					Symbol: string(symbol), Limit: day.Count,
					Type: BINANCE_DOWNLOAD_KLINE_REQUEST,
					URL:  fmt.Sprintf(publicUrl, "daily", correctedSymbol, tf, correctedSymbol, tf, day.Q),
				})
			}
			///////////////////////
			if start.Year() != now.Year() || start.Month() != now.Month() || start.Day() != now.Day() {
				// if start is not today
				// the DayArray already included the whole start day as a hueristic way, so
				// move start time to next day
				start = time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, time.UTC)
			}
			if end.Year() != now.Year() || end.Month() != now.Month() || end.Day() != now.Day() {
				// if end is not today
				// the DayArray already included the whole end day as a hueristic way, so
				// move end time to previous day
				end = time.Date(end.Year(), end.Month(), end.Day()-1, 0, 0, 0, 0, time.UTC)
				// fmt.Println("End is today, new end:", end)
			}
			if !slices.Contains([]types.Interval{types.W1, types.Mo1}, interval) {
				// fmt.Println("Start from midnight:")
				for _, query := range utils.ApiArray(start, startMidnight, interval) {
					// fmt.Println(query)
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: transformInterval, Interval: interval,
						Symbol: string(symbol), Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", correctedSymbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
					})
				}
				if midnightEnd.After(startMidnight) {
					// fmt.Println("End to midnight:")
					for _, query := range utils.ApiArray(midnightEnd, end, interval) {
						// fmt.Println(query)
						out = append(out, &types.SpotSourceRequest{
							TransformInterval: transformInterval, Interval: interval,
							Symbol: string(symbol), Limit: query.Count, Weight: 2,
							Type: BINANCE_KLINE_API_REQUEST,
							URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", correctedSymbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
						})
					}
				}
			}
		}
	}
	return out, nil
}
func SpotNormalRequestGenerator(in *ManagerInput) ([]*types.SpotSourceRequest, error) {
	out := make([]*types.SpotSourceRequest, 0)
	symbols, interval, limit := in.Symbols, in.Interval, in.Limit
	tf := utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval]
	var userEnteredLimit bool
	if limit == 0 {
		start, end := utils.InputToTimeFromIn(in)
		limit = findLimit(interval, start, end)
	} else {
		userEnteredLimit = true
	}
	var weight uint64 = 2
	urls := SPOT_API_URLS
	publicUrl := SPOT_KLINE_PUBLIC_URL

	for symbol, oldest := range symbols {
		symbol := strings.ReplaceAll(string(symbol), "_FUTURES", "")
		start, end := utils.InputToTimeFromIn(in)
		if limit > 0 && limit <= 1000 {
			out = append(out, &types.SpotSourceRequest{
				TransformInterval: interval,
				Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
				Type: BINANCE_KLINE_API_REQUEST,
				URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&limit=%d&endTime=%d", symbol, tf, limit, end.UnixMilli()),
			})
		} else if limit > 1000 {
			if userEnteredLimit && (in.EndDate == "" || in.EndTime == "") {
				startMilli := end.UnixMilli() - int64(limit)*utils.INTERVAL_MS[interval]
				for _, query := range utils.ApiArrayMilli(startMilli, end.UnixMilli(), interval) {
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: interval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, tf, query.Q),
					})
				}
				return out, nil
			} else if userEnteredLimit && in.EndDate != "" && in.EndTime != "" {
				// User entered limit and end time explicitly.
				// We don't care for start time for now
				// Since users enter this mode with small limits, we only generate query (no monthly and no daily)
				endMilli := end.UnixMilli()
				startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
				for _, query := range utils.ApiArrayMilli(startMilli, endMilli, interval) {
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: interval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, tf, query.Q),
					})
				}
				return out, nil
			}
			// User didn't enter limit, so we go with the start time and the end time
			now := time.Now()
			// Move start forward to 23:59:59 of that day
			startMidnight := time.Date(start.Year(), start.Month(), start.Day(), 23, 59, 59, 1e9-1, time.UTC)
			if startMidnight.After(end) {
				startMidnight = end
			}
			// Move end backward to 00:00:00 of that day
			midnightEnd := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
			if midnightEnd.Before(start) {
				midnightEnd = start
			}
			///////////////////////
			monthArray, didWeMoveMonth := utils.MonthArray(start, end, interval, symbol, string(oldest))
			for _, month := range monthArray {
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: interval, Interval: interval,
					Symbol: string(symbol), Limit: month.Count,
					Type: BINANCE_DOWNLOAD_KLINE_REQUEST,
					URL:  fmt.Sprintf(publicUrl, "monthly", symbol, tf, symbol, tf, month.Q),
				})
			}
			if didWeMoveMonth && (start.Year() != now.Year() || start.Month() != now.Month()) {
				// if start is not this month
				// the MonthArray already included the whole start month as a hueristic way, so
				// move start time to first day of next month
				start = time.Date(start.Year(), start.Month()+time.Month(len(monthArray)), 1, 0, 0, 0, 0, time.UTC)
			}
			dailyArray := utils.DayArray(start, end, interval)
			for _, day := range dailyArray {
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: interval, Interval: interval,
					Symbol: string(symbol), Limit: day.Count,
					Type: BINANCE_DOWNLOAD_KLINE_REQUEST,
					URL:  fmt.Sprintf(publicUrl, "daily", symbol, tf, symbol, tf, day.Q),
				})
			}
			///////////////////////
			if start.Year() != now.Year() || start.Month() != now.Month() || start.Day() != now.Day() {
				// if start is not today
				// the DayArray already included the whole start day as a hueristic way, so
				// move start time to next day
				start = time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, time.UTC)
			}
			if end.Year() != now.Year() || end.Month() != now.Month() || end.Day() != now.Day() {
				// if end is not today
				// the DayArray already included the whole end day as a hueristic way, so
				// move end time to previous day
				end = time.Date(end.Year(), end.Month(), end.Day()-1, 0, 0, 0, 0, time.UTC)
				// fmt.Println("End is today, new end:", end)
			}
			if !slices.Contains([]types.Interval{types.W1, types.Mo1}, interval) {
				// fmt.Println("Start from midnight:")
				for _, query := range utils.ApiArray(start, startMidnight, interval) {
					// fmt.Println(query)
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: interval, Interval: interval,
						Symbol: string(symbol), Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
					})
				}
				if midnightEnd.After(startMidnight) {
					// fmt.Println("End to midnight:")
					for _, query := range utils.ApiArray(midnightEnd, end, interval) {
						// fmt.Println(query)
						out = append(out, &types.SpotSourceRequest{
							TransformInterval: interval, Interval: interval,
							Symbol: string(symbol), Limit: query.Count, Weight: 2,
							Type: BINANCE_KLINE_API_REQUEST,
							URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
						})
					}
				}
			}
		}
	}
	return out, nil
}
func SpotRequestGenerator(in *ManagerInput) ([]*types.SpotSourceRequest, error) {
	if slices.Contains([]types.Interval{types.S1, types.S5, types.S15, types.S30}, in.Interval) {
		return SpotTransformRequestGeneratorV2(in)
	}
	return SpotNormalRequestGenerator(in)
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func FutureApiRequestGenerator(in *ManagerInput) ([]*types.FutureSourceRequest, error) {
	out := make([]*types.FutureSourceRequest, 0)
	apiUrl := FUTURES_API_URLS[0]
	symbols, limit := in.Symbols, in.Limit
	if slices.Contains([]types.Interval{types.S1, types.S5, types.S15, types.S30}, in.Interval) {
		intervals := in.Intervals
		if len(in.Intervals) == 0 {
			intervals = []types.Interval{in.Interval}
		}
		var weight uint64 = 20

		for symbol := range symbols {
			start, end := utils.InputToTimeFromIn(in)

			for _, interval := range intervals {

				if limit != 0 && (in.StartDate == "" || in.StartTime == "") {
					// User entered limit explicitly.
					// Since we need to consider limit, but the API only allows us to enter end time, we calculate the start time based on the end time and the limit and ignore the start time entered by user.
					startMilli := end.UnixMilli() - int64(limit)*utils.INTERVAL_MS[interval]
					start = time.UnixMilli(startMilli)
				}
				// If user has entered all of start time, end time and limit, limit parameter is going to be ignored since we have end time and start time defined.
				// Avoid using this mode
				out = append(out, &types.FutureSourceRequest{
					Intervals: intervals,
					Symbol:    string(symbol), Limit: limit, Weight: weight,
					Type:  BINANCE_TRADE_API_REQUEST,
					URL:   FUTURES_TRADE_API_URL,
					Start: start, End: end,
				})
			}
		}
		return out, nil
	} else {
		/////////////////////////////////////////////////////////
		interval := in.Interval
		tf := utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval]
		start, end := utils.InputToTimeFromIn(in)
		var userEnteredLimit bool
		if limit == 0 {
			start, end := utils.InputToTimeFromIn(in)
			limit = findLimit(interval, start, end)
		} else {
			userEnteredLimit = true
		}
		var weight uint64
		switch {
		case limit > 0 && limit < 100:
			weight = 1
		case limit >= 100 && limit < 500:
			weight = 2
		case limit >= 500 && limit <= 1000:
			weight = 5
		case limit > 1000:
			weight = 10
		}
		for symbol := range symbols {
			correctedSymbol := strings.ReplaceAll(string(symbol), "_FUTURES", "")
			if limit > 0 && limit <= 1000 {
				out = append(out, &types.FutureSourceRequest{
					TransformInterval: interval,
					Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: BINANCE_KLINE_API_REQUEST,
					URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&limit=%d&endTime=%d", correctedSymbol, tf, limit, end.UnixMilli()),
				})
			} else if limit > 1000 {
				if userEnteredLimit {
					endMilli := end.UnixMilli()
					startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
					start = time.UnixMilli(startMilli)
				}
				for _, query := range utils.ApiArrayMilli(start.UnixMilli(), end.UnixMilli(), interval) {
					out = append(out, &types.FutureSourceRequest{
						TransformInterval: interval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&%s", correctedSymbol, tf, query.Q),
					})
				}
			}
		}
		return out, nil
	}
}
func SpotApiRequestGenerator(in *ManagerInput) ([]*types.SpotSourceRequest, error) {
	out := make([]*types.SpotSourceRequest, 0)
	apiUrl := SPOT_API_URLS[0]
	symbols, limit := in.Symbols, in.Limit
	if slices.Contains([]types.Interval{types.S1, types.S5, types.S15, types.S30}, in.Interval) {
		transformIntervals := in.Intervals
		if len(in.Intervals) == 0 {
			transformIntervals = []types.Interval{in.Interval}
		}
		var weight uint64 = 2
		for symbol := range symbols {
			start, end := utils.InputToTimeFromIn(in)
			if limit != 0 && (in.StartDate == "" || in.StartTime == "") {
				// User entered limit explicitly.
				// Since we need to consider limit, but the API only allows us to enter end time, we calculate the start time based on the end time and the limit and ignore the start time entered by user.
				startMilli := end.UnixMilli() - int64(limit)*utils.INTERVAL_MS[types.S1]
				start = time.UnixMilli(startMilli)
			}
			out = append(out, &types.SpotSourceRequest{
				Intervals: transformIntervals, Interval: types.S1,
				Symbol: string(symbol), Limit: 0, Weight: weight,
				Start: start, End: end,
				Type: SPOT_API_WITH_TRANSFORM_REQUEST,
				// URL:  fmt.Sprintf(urls[rand.IntN(len(urls))]+"?symbol=%s&interval=%s&%s", symbol, utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval], query.Q),
				URL: apiUrl,
			})

		}

		return out, nil
	} else {
		interval := in.Interval
		tf := utils.INTERVAL_TIMEFRAME_MAP[config.C.Market()][interval]
		start, end := utils.InputToTimeFromIn(in)
		var userEnteredLimit bool
		if limit == 0 {
			start, end := utils.InputToTimeFromIn(in)
			limit = findLimit(interval, start, end)
		} else {
			userEnteredLimit = true
		}
		var weight uint64 = 2
		for symbol := range symbols {
			if limit > 0 && limit <= 1000 {
				out = append(out, &types.SpotSourceRequest{
					TransformInterval: interval,
					Symbol:            string(symbol), Interval: interval, Limit: limit, Weight: weight,
					Type: BINANCE_KLINE_API_REQUEST,
					URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&limit=%d&endTime=%d", string(symbol), tf, limit, end.UnixMilli()),
				})
			} else if limit > 1000 {
				if userEnteredLimit {
					endMilli := end.UnixMilli()
					startMilli := endMilli - int64(limit)*utils.INTERVAL_MS[interval]
					start = time.UnixMilli(startMilli)
				}
				for _, query := range utils.ApiArrayMilli(start.UnixMilli(), end.UnixMilli(), interval) {
					out = append(out, &types.SpotSourceRequest{
						TransformInterval: interval,
						Symbol:            string(symbol), Interval: interval, Limit: query.Count, Weight: 2,
						Type: BINANCE_KLINE_API_REQUEST,
						URL:  fmt.Sprintf(apiUrl+"?symbol=%s&interval=%s&%s", string(symbol), tf, query.Q),
					})
				}
			}
		}
		return out, nil
	}
}
*/

/*
func ApiRequestGenerator(in *ManagerInput) ([]*types.SourceRequest, error) {
	if config.C.Market() == utils.FOREX_MARKET {
		return ForexRequestGenerator(in)
	} else if config.C.Market() == utils.NOBITEX_MARKET || config.C.Market() == utils.BITYCLE_MARKET {
		return NobitexRequestGenerator(in)
	} else if config.C.Market() == utils.FUTURES_MARKET {
		return FutureApiRequestGenerator(in)
	}
	return SpotApiRequestGenerator(in)
}
*/
