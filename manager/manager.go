package manager

import (
	"context"
)

type Manager struct {
	forexDispatcher *ForexDispatcher
}

func New(ctx context.Context) (*Manager, error) { //, emitter *sink.Emitter

	// forexDispatcher, err := NewForexDispatcher(ctx, m.forexPipeEntry)
	forexDispatcher, err := NewForexDispatcher(ctx)
	if err != nil {
		return nil, err
	}

	m := &Manager{forexDispatcher: forexDispatcher}
	return m, nil
}

/*
func (m *Manager) DoAsync(ctx context.Context, input *ManagerInput) <-chan *types.Result {
	input.Id = rand.Int64()
	tempResultCh := make(chan *types.Result, 2)
	defer close(tempResultCh)
	if config.C.App() == "history" {
		fmt.Println("Input -----------------------",
			"\n\tsymbol:", input.Symbols,
			"\n\tinterval:", input.Interval,
			"\n\tintervals:", input.Intervals,
			"\n\tstart:", input.StartDate, input.StartTime,
			"\n\tend:", input.EndDate, input.EndTime,
			"\n\tlimit:", input.Limit,
			"\n\tgap:", input.Gap,
		)
	}
	result := &types.Result{ID: input.Id, Intervals: input.Intervals, Errs: make([]error, 0)}
	if len(result.Intervals) == 0 {
		result.Intervals = append(result.Intervals, input.Interval)
	}
	// if input.Gap {
	// 	manager.convertToGapRequests(input)
	// }
	requests, err := RequestGenerator(input)
	if err != nil {
		result.Errs = append(result.Errs, err)
		tempResultCh <- result
		return tempResultCh
	} else if len(requests) == 0 {
		result.Errs = append(result.Errs, fmt.Errorf("No request can be generated with this input %v", *input))
		tempResultCh <- result
		return tempResultCh
	}
	// if config.C.App() == "history" && originalRequest.DeleteFirst {
	// 	start, end := utils.InputToTimeFromIn(originalRequest)
	// 	if originalRequest.Interval == types.NoInterval {
	// 		for _, interval := range originalRequest.Intervals {
	// 			if originalRequest.Interval <= types.S30 {
	// 				manager.emitter.Delete(originalRequest.Symbols, interval, start, end)
	// 			}
	// 		}
	// 	} else if originalRequest.Interval <= types.S30 {
	// 		manager.emitter.Delete(originalRequest.Symbols, originalRequest.Interval, start, end)
	// 	}
	// }
	///////////////// Scheduling requests to be done
	sourceResultChs := make([]<-chan *types.SourceResult, 0)
	for _, request := range requests {
		if request == nil {
			continue
		}
		sourceResultCh := m.dispatch(ctx, request, input)
		sourceResultChs = append(sourceResultChs, sourceResultCh)

	}
	//////////////// Set up a pipeline to gather source results and aggregate them into one final result
	return utils.Reduce(
		result,
		utils.FanInOnce(sourceResultChs...),
		func(result *types.Result, v *types.SourceResult) {
			result.Count += v.Count
			result.Retried += v.Retried
			if v.Err != nil {
				result.Errs = append(result.Errs, v.Err)
			}
		})
}

func (manager *Manager) check(mode types.REQUEST_MODE, weight uint64) bool {
	switch mode {
	case DOWNLOAD:
		return manager.ccd.Load() < int64(config.C.MaxCuncurrentDownload())
	case API:
		ok, err := manager.sp.TryConsume("Manager says", weight)
		if err != nil {
			logrus.Warnln("tryconsume error:", err)
		}
		return ok
	}
	return true
}
func (manager *Manager) checkRetry(mode types.REQUEST_MODE, weight uint64, minWait time.Duration) bool {
	for {
		switch mode {
		case SPOT_API_WITH_TRANSFORM_REQUEST, BINANCE_KLINE_API_REQUEST, BINANCE_TRADE_API_REQUEST, TV_SOCKET_REQUEST, NOBITEX_API_REQUEST:
			d := max(time.Until(time.Now().Truncate(time.Minute).Add(time.Minute).Add(10*time.Second)), minWait)
			time.Sleep(d)
			ok, err := manager.sp.TryConsume("Check for retry", weight)
			if err != nil {
				logrus.Warnln("tryconsume error:", err)
			}
			if ok {
				return true
			}
		case BINANCE_DOWNLOAD_KLINE_REQUEST, SPOT_DOWNLOAD_WITH_TRANSFORM_REQUEST, BINANCE_DOWNLOAD_TRADE_REQUEST:
			fallthrough
		default:
			time.Sleep(minWait)
			return true
		}
	}
}

func (manager *Manager) convertToGapRequests(requestInput *types.Input) {
	id := rand.IntN(1000)
	go func() {
		if requestInput.StartDate == "" || requestInput.StartTime == "" || requestInput.EndDate == "" || requestInput.EndTime == "" {
			select {
			case requestInput.ErrCh <- fmt.Errorf("No request can be generated with this input %v", *requestInput):
			default:
				logrus.Warnln("No request can be generated with this input", *requestInput)
			}
			return
		}
		start, end := utils.InputToTimeFromIn(requestInput)
		// for symbol := range requestInput.Symbols {
		logrus.Infoln("Finding Gap...")
		gaps, err := manager.emitter.FindGapsForSymbols(requestInput.Symbols, requestInput.Interval, start, end)
		logrus.Infoln("Finding Gap finished!!!")
		if len(gaps) == 0 {
			return
		}
		if err != nil {
			select {
			case requestInput.ErrCh <- err:
			default:
				logrus.Warnln(err)
			}
			return
		}
		for _, gap := range gaps {
			gapInput := &types.Input{
				ResCh:       requestInput.ResCh,
				ErrCh:       requestInput.ErrCh,
				Symbols:     map[types.Symbol]types.Oldest{types.Symbol(gap.Symbol): config.C.SymbolCache().Symbols[types.Symbol(gap.Symbol)]},
				Interval:    gap.Interval,
				StartDate:   gap.Start.Format("2006-01-02"),
				StartTime:   gap.Start.Format("15:04:05"),
				EndDate:     gap.End.Format("2006-01-02"),
				EndTime:     gap.End.Format("15:04:05"),
				Limit:       0,
				Gap:         false,
				DeleteFirst: false,
			}
			if gapRequests, err := ApiRequestGenerator(gapInput); err == nil {
				manager.threaded(id, gapRequests, requestInput)
				// for _, gapRequest := range gapRequests {
				// 	fmt.Println("Gap:", gapRequest)
				// }
			} else {
				select {
				case requestInput.ErrCh <- err:
				default:
					logrus.Warnln(err)
				}
			}
		}
		// }
	}()
}

// ///////////////////////////////////////////////////////////////////////////////////

type Status struct {
	Download struct {
		Pending int   `json:"pending"`
		Used    int64 `json:"used"`
	} `json:"download"`
	API struct {
		Pending    int   `json:"pending"`
		UsedWeight int64 `json:"weight_used"`
	} `json:"api"`
}

func (manager *Manager) Status() Status {
	s := Status{}
	s.Download = struct {
		Pending int   `json:"pending"`
		Used    int64 `json:"used"`
	}{
		Pending: len(manager.multiLevelPending.L0),
		Used:    manager.ccd.Load(),
	}
	_, weight := manager.sp.Read()
	s.API = struct {
		Pending    int   `json:"pending"`
		UsedWeight int64 `json:"weight_used"`
	}{
		Pending:    len(manager.multiLevelPending.L1),
		UsedWeight: int64(weight),
	}
	return s
}
*/
