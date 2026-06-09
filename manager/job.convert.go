package manager

import (
	"math/rand/v2"
	"time"

	"github.com/HamedMolavi/finance-data-stream/config"
	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/HamedMolavi/finance-data-stream/types"
	"github.com/sirupsen/logrus"
)

func generateInputId() int64 { return rand.Int64() }
func convertTimeframes(body *JobBody) []types.Interval {
	var intervals []types.Interval
	if len(body.Timeframes) == 0 {
		intervals = types.INTERVALS
	} else {
		for _, tf := range body.Timeframes {
			if interval, ok := types.SYSTEM_TIMEFRAME_INTERVAL_MAP[tf]; ok {
				intervals = append(intervals, interval)
			} else {
				logrus.Warnln("Wrong interval in input", tf)
			}
		}
	}
	return intervals
}
func convertSymbols(body *JobBody) map[types.Symbol]types.Oldest {
	var symbols map[types.Symbol]types.Oldest
	if len(body.Symbols) == 0 {
		symbols = config.C.SymbolCache().Symbols
	} else {
		symbols = make(map[types.Symbol]types.Oldest, len(body.Symbols))
		for _, sym := range body.Symbols {
			if oldest, ok := config.C.SymbolCache().Symbols[types.Symbol(sym)]; ok {
				symbols[types.Symbol(sym)] = oldest
			} else {
				logrus.Warnln("Wrong symbol input", sym)
			}
		}
	}
	return symbols
}
func convertTimes(body *JobBody) (start, end *ManagerTime) {
	var err error
	var startF, endF bool
	var startT, endT time.Time
	if body.Start == "" || body.Start == "0" {
		body.Start = "2017-07-01 00:00:00"
	} else {
		startF = true
	}
	startT, err = time.Parse(types.DATE_TIME_LAYOUT, body.Start)
	if err != nil {
		startT = time.Date(2017, 07, 01, 0, 0, 0, 0, time.UTC)
		startF = false
	}
	endT, err = time.Parse(types.DATE_TIME_LAYOUT, body.End)
	if err != nil {
		endF = false
		endT = time.Now().Add(-time.Minute)
	}
	if endT.After(time.Now().UTC()) {
		endF = false
		endT = time.Now().Add(-time.Minute)
	}
	return &ManagerTime{T: startT, F: startF}, &ManagerTime{T: endT, F: endF}
}

var JobConversionStage = pipeline.MapStageFactory(
	func(body *JobBody) *ManagerInput {
		intervals := convertTimeframes(body)
		symbols := convertSymbols(body)
		start, end := convertTimes(body)
		return &ManagerInput{
			Id:          generateInputId(),
			Symbols:     symbols,
			Intervals:   intervals,
			Start:       start,
			End:         end,
			Limit:       body.Limit,
			DeleteFirst: body.DeleteFirst,
		}

		/*
			if config.C.Market() == utils.FOREX_MARKET {
			} else if (config.C.Market() == utils.FUTURES_MARKET || config.C.Market() == utils.SPOT_MARKET) && !body.Gap {
				sIntervals := make([]types.Interval, 0)
				mIntervals := make([]types.Interval, 0)
				for _, interval := range intervals {
					if interval < types.M1 {
						sIntervals = append(sIntervals, interval)
					} else {
						mIntervals = append(mIntervals, interval)
					}
				}
				if len(sIntervals) > 0 {
					inCh <- &ManagerInput{
						ResCh:       resCh,
						ErrCh:       errCh,
						Symbols:     symbols,
						Interval:    types.S1, // base interval to make other intervals of it
						Intervals:   sIntervals,
						EndDate:     body.EndDate,
						EndTime:     body.EndTime,
						Start:   body.Start,
						StartTime:   body.StartTime,
						Limit:       body.Limit,
						Gap:         body.Gap,
						DeleteFirst: body.DeleteFirst,
					}
					i++
				}
				if len(mIntervals) > 0 {
					for _, interval := range mIntervals {
						inCh <- &ManagerInput{
							ResCh:       resCh,
							ErrCh:       errCh,
							Symbols:     symbols,
							Interval:    interval,
							EndDate:     body.EndDate,
							EndTime:     body.EndTime,
							Start:   body.Start,
							StartTime:   body.StartTime,
							Limit:       body.Limit,
							Gap:         body.Gap,
							DeleteFirst: body.DeleteFirst,
						}
						i++
					}
				}
			} else {
				for _, interval := range intervals {
					inCh <- &ManagerInput{
						Symbols:     symbols,
						Interval:    interval,
						EndDate:     body.EndDate,
						EndTime:     body.EndTime,
						Start:   body.Start,
						StartTime:   body.StartTime,
						Limit:       body.Limit,
						Gap:         body.Gap,
						DeleteFirst: body.DeleteFirst,
					}
				}
			}
			go func() {
				for ; i > 0; i-- {
					select {
					case res := <-resCh:
						totalRes := make(map[types.Interval]int, len(utils.INTERVALS))
						for _, result := range res {
							for _, interval := range result.Intervals {
								totalRes[interval] += int(result.Count) / len(result.Intervals)
							}
						}
						resStr := ""
						for interval, count := range totalRes {
							resStr = resStr + fmt.Sprintf(" interval:%v inserted:%d ", interval, count)
						}
						logrus.Infoln("One candle fix done", resStr)
					case err := <-errCh:
						logrus.Error("Error returned by fix manager", err)
					}
				}
			}()
		*/

	},
)
