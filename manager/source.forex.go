package manager

import (
	"context"
	"time"

	"github.com/HamedMolavi/finance-data-stream/config"
	"github.com/HamedMolavi/finance-data-stream/lock"
	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/HamedMolavi/finance-data-stream/tradingview"
	"github.com/HamedMolavi/finance-data-stream/types"
	"github.com/sirupsen/logrus"
)

type ForexDispatcher struct {
	sp *lock.SharedToken
}

func NewForexDispatcher(ctx context.Context) (*ForexDispatcher, error) {
	sp, err := lock.OpenSharedToken(ctx, "forex_perms.bin", config.C.MaxWeight(), time.Minute)
	if err != nil {
		return nil, err
	}
	fd := &ForexDispatcher{
		sp: sp,
	}
	// go fd.loop()
	return fd, nil
}

// func (fd *ForexDispatcher) loop(ctx context.Context) {
// 	for j := range fd.queue {
// 		// do the request
// 		j.resultCh <- &sourceResult{Count: 0, Retried: 0, Err: nil}
// 		close(j.resultCh)
// 	}
// }

type routingTableKey struct {
	symbol   types.Symbol
	interval types.Interval
}

func WouldYouFuckMe(request *ForexSourceRequest) *pipeline.RoutingTable[types.Interval, *types.Kline] {
	socket := tradingview.NewSocket()
	if err := socket.Authorize(config.C.HistoryToken()); err != nil {
		logrus.Warnln("Tradingview Authentication failed (write):", err)
	}
	socket.SocketIsFuckingMe(context.TODO())
	///

	routingTable := make(map[routingTableKey]pipeline.Stream[*types.Kline], len(request.Intervals)*len(request.Symbols))
	for _, interval := range request.Intervals {
		for _, symbol := range request.Symbols {
			pipe, err := socket.SessionStageFactory(symbol, interval, request.Limit)
			if err != nil {
				logrus.Warnln(err)
				continue
			}
			routingTable[routingTableKey{symbol: symbol, interval: interval}] = pipe
		}
	}
	///

	return RerouteStage(routingTable)
}

func RerouteStage(table map[routingTableKey]pipeline.Stream[*types.Kline], so ...pipeline.StageOption) *pipeline.RoutingTable[types.Interval, *types.Kline] {
	sendTable := make(map[types.Interval]chan *types.Kline)
	for key, inbound := range table {
		outbound, ok := sendTable[key.interval]
		if !ok {
			outbound = make(chan *types.Kline, 100)
			sendTable[key.interval] = outbound
		}
		go func(inbound pipeline.Stream[*types.Kline]) {
			defer close(outbound)
			for k := range inbound {
				outbound <- k
			}
		}(inbound)
	}
	return pipeline.NewRoutingTable(sendTable)
}

/*

	type FilteredMessage struct {
		DU []byte
		SC []byte
	}

	func FilterStageFactory(socket *tradingview.Socket) pipeline.TryProcessStage[[]byte, *FilteredMessage] {
		return func(eFunc pipeline.ErrorRegistererFunc, reader pipeline.Stream[[]byte], opts ...pipeline.StageOption) pipeline.Stream[*FilteredMessage] {
			cfg := pipeline.CreateConfig(opts)
			errCh := make(chan *pipeline.Error, cap(reader))
			eFunc(errCh)
			outbound := make(chan *FilteredMessage, 100)
			go func() {
				defer func() {
					if err := recover(); err != nil {
						logrus.Warnln("manager.FilterStage Goroutine paniced:", err)
					}
				}()
				defer close(errCh)
				defer close(outbound)
				for payload := range reader {
					text := string(payload)
					// non-numeric last char
					re := regexp.MustCompile(`~m~[0-9]+~m~`)
					// Split the input string on the delimiter
					parts := re.Split(text, -1)
					for _, s := range parts {
						// Filter out empty/whitespace-only parts
						if strings.TrimSpace(s) == "" {
							continue
						}
						if strings.HasPrefix(s, `~h~`) {
							socket.WriteBytes([]byte(s))
						} else if strings.Contains(s, `"m":"du"`) { // I guess updates on candles
							outbound <- &FilteredMessage{DU: []byte(s)}
						} else if strings.Contains(s, `timescale_update`) { // New candle created!
							outbound <- &FilteredMessage{DU: []byte(s)}
						} else if strings.Contains(s, "series_completed") {
							outbound <- &FilteredMessage{SC: []byte(s)}
						} else if slices.ContainsFunc(tradingview.KNOWN_STOP_ERRORS, func(err string) bool { return strings.Contains(s, err) }) {
							errCh <- &pipeline.Error{
								Error:       errors.New(s),
								StageConfig: cfg,
								Input:       payload,
								Output:      s,
							}
						} else {
							// fmt.Println(s)
						}
					}
				}
			}()
			return outbound
		}
	}

	type UnmarshaledMessage struct {
		DU   *tradingview.Message
		COMP chan int
	}

	func FanoutMessageStageFactory(socket *tradingview.Socket, chartSessions []tradingview.ChartSessionID) pipeline.ProcessRouterStage[*FilteredMessage, tradingview.ChartSessionID, *UnmarshaledMessage] {
		sendTable := make(map[tradingview.ChartSessionID]chan *UnmarshaledMessage)
		for _, chartSession := range chartSessions {
			sendTable[chartSession] = make(chan *UnmarshaledMessage, 100)
		}

		return func(inbound pipeline.Stream[*FilteredMessage], so ...pipeline.StageOption) *pipeline.RoutingTable[tradingview.ChartSessionID, *UnmarshaledMessage] {
			go func() {
				defer func() {
					if err := recover(); err != nil {
						logrus.Warnln("manager.FanoutStage Goroutine paniced:", err)
					}
				}()
				regex := regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)
				for t := range inbound {
					switch {
					case t.DU != nil:
						msg, err := tradingview.UnmarshalMessage(t.DU)
						if err != nil {
							logrus.Errorf("failed to unmarshal JSON segment: %v; segment: |%s|", err, t.DU)
							continue
						}
						if len(msg.P.Sds.Sds1.Candles) > 0 {
							chartSession := tradingview.ChartSessionID(msg.P.ChartSessionID)
							if route, ok := sendTable[chartSession]; ok {
								route <- &UnmarshaledMessage{DU: msg}
							} else {
								logrus.Warnln("wrong chartsession in worker", chartSession)
							}
						}
					case t.SC != nil:
						// fmt.Println("complete message came", complete)
						result := regex.FindStringSubmatch(string(t.DU))
						if len(result) > 1 {
							chartSession := tradingview.ChartSessionID(result[1])
							// fmt.Println("Worker detected series completion", chartSession)
							if route, ok := sendTable[chartSession]; ok {
								reply := make(chan int)
								route <- &UnmarshaledMessage{COMP: reply}
								finished := <-reply
								if finished > 0 {
									// fmt.Println("Worker 2 realized history hasn't finished yet", chartSession, finished)
									socket.WriteBytes(fmt.Appendf(nil, `{"m":"request_more_data","p":["%s","sds_1",%d]}`, chartSession, finished))
								} else {
									// return
								}
							}
						} else {
							logrus.Warnln("complete message came but didn't have a chart session id", t.DU)
						}
					}
				}
			}()
			return pipeline.NewRoutingTable(sendTable)
		}
	}
*/

/*
func ErrorHandler(socket *websocket.Conn, errorCh chan string, forceStopProxyChs map[string]chan struct{}) {
	for err := range errorCh {
		var errStr string
		var i int
		for i, errStr = range tradingview.KNOWN_STOP_ERRORS {
			if strings.Contains(err, errStr) {
				break
			}
			if i == len(tradingview.KNOWN_STOP_ERRORS)-1 {
				errStr = ""
			}
		}
		if errStr != "" {
			m := chartSessionRe.FindStringSubmatch(err)
			if len(m) == 2 {
				chartSession := strings.TrimSpace(m[1])
				if forceStopProxyCh, ok := forceStopProxyChs[chartSession]; ok {
					logrus.Warnln("Stopping", "symbol interval", "because of", errStr)
					forceStopProxyCh <- struct{}{}
				} else {
					logrus.Warnln(errStr, "; unknown chart session", chartSession)
				}
			} else {
				logrus.Warnln("Unable to retrieve information from", errStr, "error", err)
			}
		} else {
			logrus.Warnln("Unknown error", err)
		}
	}
}
*/
