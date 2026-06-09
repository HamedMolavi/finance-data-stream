package tradingview

/*
import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/HamedMolavi/finance-data-stream/config"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var logSymbol = config.C.LogSymbol()
var logInterval = config.C.LogInterval()

type SymbolIntervalTuple struct {
	S string
	I types.Interval
}
type TradingViewSocket struct {
	banned atomic.Bool
	// conn            *websocket.Conn
	chartSessionMap map[string]*SymbolIntervalTuple
	show            bool
}

func NewTradingViewLiveSocket(klineChs map[types.Interval]chan *types.Kline, managerInCh chan *types.Input, doneCh <-chan chan struct{}, parentSymbols []string, show bool) {
	socket := &TradingViewSocket{
		show: show,
	}
	fuckingTypedSymobls := make(map[types.Symbol]types.Oldest, len(parentSymbols))
	for _, sym := range parentSymbols {
		fuckingTypedSymobls[types.Symbol(sym)] = types.Oldest("")
	}
	helper := func(symbols []string, closeCh, reconnectCh chan struct{}, writerCh, filterWorkerCh, decoderWorkerCh chan []byte, registerCh, printCh chan string) map[string]*SymbolIntervalTuple {
		u := config.C.WsBaseUrl()
		conn := NewSocket(u)
		converterChs := make(map[string]chan *Candle, len(utils.INTERVALS)*len(symbols))
		chartSessionMap := make(map[string]*SymbolIntervalTuple, len(utils.INTERVALS)*len(symbols))
		m1ChartSession := ""
		// fmt.Println("Socket connected")
		//////////////////////////////////////////////////////////
		for _, symbol := range symbols {
			/////////////////////
			// for each interval:
			for _, interval := range utils.INTERVALS {
				if timeFrame, ok := utils.INTERVAL_TIMEFRAME_MAP[utils.FOREX_MARKET][interval]; ok {
					copiedInterval := types.Interval(interval)
					klineCh := klineChs[copiedInterval]
					chartSession := GenerateRandomString("cs_", 12)
					if copiedInterval == types.M1 {
						m1ChartSession = chartSession
					}
					converterCh := make(chan *Candle, 1)
					converterChs[chartSession] = converterCh
					chartSessionMap[chartSession] = &SymbolIntervalTuple{symbol, copiedInterval}
					go socket.converter(symbol, copiedInterval, converterCh, klineCh)
					for _, message := range SetupLiveMessages(timeFrame, symbol, chartSession) {
						err := socket.WriteMessage(conn, message)
						if err != nil {
							logrus.Errorln("sent setup message", err)
							// TODO: what to do in case of setup error? Right now I restart socket
							reconnectCh <- struct{}{}
						}
					}
				}
			}
		}
		//////////////////////////////////////////////////////////
		go socket.writePump(conn, symbols, writerCh)
		go socket.readPump(conn, symbols, filterWorkerCh, writerCh, reconnectCh)
		go socket.filterWorker(filterWorkerCh, decoderWorkerCh, writerCh, closeCh, reconnectCh, registerCh, printCh, chartSessionMap, m1ChartSession)
		go socket.decodeWorker(decoderWorkerCh, converterChs)
		// for range 20 {
		// 	go socket.filterWorker(filterWorkerCh, decoderWorkerCh, writerCh, closeCh, reconnectCh, registerCh, printCh, chartSessionMap, m1ChartSession)
		// }
		// for range 50 {
		// 	go socket.decodeWorker(decoderWorkerCh, converterChs)
		// }
		fmt.Println("TV socket connected", symbols[0])
		return chartSessionMap
	}
	registerCh := make(chan string, 1)
	printCh := make(chan string, 1)
	reconnectCh := make(chan struct{}, 1) // recreate the reconnect channel to avoid reading old related reconnect messages
	closeCh := make(chan struct{}, 1)     // recreate the close channel to avoid reading old related close messages
	writerCh := make(chan []byte)
	filterWorkerCh := make(chan []byte, 10*len(utils.INTERVALS))
	decodeWorkerCh := make(chan []byte, 10*len(utils.INTERVALS))
	socket.chartSessionMap = helper(parentSymbols, closeCh, reconnectCh, writerCh, filterWorkerCh, decodeWorkerCh, registerCh, printCh)
	go func() {
	loop:
		for {
			select {
			case rawMsg := <-printCh:
				regex := regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)
				result := regex.FindStringSubmatch(rawMsg)
				if len(result) > 1 {
					chartSession := result[1]
					if symInterval, ok := socket.chartSessionMap[chartSession]; ok {
						logrus.Errorln("got error on", symInterval.S, symInterval.I, rawMsg)
					}
				}
				continue loop
			case <-registerCh:
				// regex := regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)
				// result := regex.FindStringSubmatch(rawMsg)
				// if len(result) > 1 {
				// 	chartSession := result[1]
				// 	if symInterval, ok := socket.chartSessionMap[chartSession]; ok {
				// 		timeFrame := utils.INTERVAL_TIMEFRAME_MAP[utils.FOREX_MARKET][symInterval.I]
				// 		chartSession := GenerateRandomString("cs_", 12)
				// 		for _, message := range SetupLiveMessages(timeFrame, symInterval.S, chartSession) {
				// 			st, err := json.Marshal(message)
				// 			if err != nil {
				// 				continue
				// 			}
				// 			// append headers
				// 			s := fmt.Appendf(nil, "~m~%d~m~%s", len(st), string(st))
				// 			writerCh <- s
				// 		}
				// 		socket.chartSessionMap[chartSession] = symInterval
				// 	}
				// }
				continue loop
			case replyCh := <-doneCh:
				fmt.Println("Done request received in socket", parentSymbols[0])
				close(writerCh)                                   // will cause Writer to close the connection
				time.Sleep(time.Duration(100) * time.Millisecond) // wait for the reader to close
				close(filterWorkerCh)                             // by closing filter channel we make the worker to return
				close(decodeWorkerCh)                             // by closing decoder channel we make the worker to return
				replyCh <- struct{}{}
				return
			case <-closeCh:
				close(writerCh)                                   // will cause Writer to close the connection
				time.Sleep(time.Duration(100) * time.Millisecond) // wait for the reader to close
				close(filterWorkerCh)                             // by closing filter channel we make the worker to return
				close(decodeWorkerCh)                             // by closing decoder channel we make the worker to return
				return
			case <-reconnectCh:
				// Reader already closed
				logrus.Warnln(parentSymbols[0], "Tradingview socket reconnect called, banned", socket.banned.Load())

				close(writerCh)                                   // will cause Writer to close the connection
				time.Sleep(time.Duration(100) * time.Millisecond) // wait for the reader to close
				close(filterWorkerCh)                             // by closing filter channel we make the worker to return
				close(decodeWorkerCh)                             // by closing decoder channel we make the worker to return
				registerCh = make(chan string, 1)
				printCh = make(chan string, 1)
				reconnectCh = make(chan struct{}, 1) // recreate the reconnect channel to avoid reading old related reconnect messages
				closeCh = make(chan struct{}, 1)     // recreate the close channel to avoid reading old related close messages
				writerCh = make(chan []byte)
				filterWorkerCh = make(chan []byte, 10*len(utils.INTERVALS))
				decodeWorkerCh = make(chan []byte, 10*len(utils.INTERVALS))
				if socket.banned.Load() {
					config.C.SwapTokens()
					// time.Sleep(time.Minute + time.Duration(rand.IntN(5))*time.Second)
				}
				socket.chartSessionMap = helper(parentSymbols, closeCh, reconnectCh, writerCh, filterWorkerCh, decodeWorkerCh, registerCh, printCh)

				go func() {
					broadResCh := make(chan []*types.Result)
					errCh := make(chan error)
					managerInCh <- &types.Input{
						ResCh:     broadResCh,
						ErrCh:     errCh,
						Limit:     120,
						Symbols:   fuckingTypedSymobls,
						Intervals: utils.INTERVALS,
					}
					select {
					case res := <-broadResCh:
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
						logrus.Infoln(parentSymbols[0], "Fixing candles in socket reconnection done", resStr)
					case err := <-errCh:
						logrus.Error("Error returned by manager", err)
					}
				}()
			}
		}
	}()
}

func (socket *TradingViewSocket) WriteMessage(conn *websocket.Conn, j any) error {
	// stringify
	st, err := json.Marshal(j)
	if err != nil {
		return err
	}
	// append headers
	message := fmt.Sprintf("~m~%d~m~%s", len(st), string(st))
	// send to socket
	err = conn.WriteMessage(websocket.BinaryMessage, []byte(message))
	return err
}
func (socket *TradingViewSocket) writePump(conn *websocket.Conn, symbols []string, writerCh <-chan []byte) {
	for payload := range writerCh {
		// reply
		if !bytes.HasPrefix(payload, []byte{126, 109, 126}) {
			l := len(payload)
			prefix := fmt.Sprintf("~m~%d~m~", l)
			payload = append(append(make([]byte, 0, len(prefix)+l), prefix...), payload...)
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte(payload)); err != nil {
			logrus.Errorf("write error replying with digit %q: %v — will not reconnect", payload, err)
			continue
		}
	}
	// When writerCh gets closed, we are done with the connection so close it.
	logrus.Warnln("Writer channel closed", symbols[0])
	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}
func (socket *TradingViewSocket) readPump(conn *websocket.Conn, symbols []string, workerCh, writerCh chan<- []byte, reconnectCh chan<- struct{}) {
	defer func() { recover() }()
	defer func() {
		select {
		case reconnectCh <- struct{}{}:
		default:
		}
	}()
	defer logrus.Warnln("read pump closed", symbols[0])
	for {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			// Read error or closed — break to reconnect
			logrus.Errorf("Tradingview socket read error: %v, type:%d, msg:%s", err, msgType, payload)
			return
		}
		workerCh <- payload
		// select {
		// case workerCh <- payload:
		// default:
		// 	logrus.Warnln("Trading view socket full filter worker cahnnel", symbols[0])
		// }
	}
}

var chartSessionRe = regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)

func (socket *TradingViewSocket) filterWorker(workerCh <-chan []byte, worker2Ch, writerCh chan<- []byte, closeCh, reconnectCh chan<- struct{}, registerCh, printCh chan<- string, chartSessionMap map[string]*SymbolIntervalTuple, m1ChartSession string) {
	defer func() { recover() }()
	for payload := range workerCh {
		text := string(payload)
		var errStr string
		if strings.Contains(text, "protocol_error") {
			if strings.Contains(text, "banned") {
				logrus.Errorln("****** BANNED")
				socket.banned.Store(true)
				reconnectCh <- struct{}{}
				return
			} else {
				logrus.Errorln("Unknown protocol error; please fix the issue before rerunning the service")
				logrus.Errorln(text)
				closeCh <- struct{}{}
				return
			}
		}
		re := regexp.MustCompile(`~m~[0-9]+~m~`)
		// Split the input string on the delimiter
		parts := re.Split(text, -1)
		for _, s := range parts {
			// Filter out empty/whitespace-only parts
			if strings.TrimSpace(s) == "" {
				continue
			}
			if strings.HasPrefix(s, `~h~`) {
				if socket.show {
					logrus.Println("Ping", s)
				}
				writerCh <- []byte(s)
			} else if strings.Contains(s, `seconds_not_entitled`) {

					// In case of invalidated token we get an error, probably this
					// ~m~134~m~{"m":"critical_error","p":["cs_casjcotuguzl","invalid parameters","method: create_series. args: \"[sds_1, s1, sds_sym_1, s1, 1, ]\""]}

				// config.C.UpdateToken(LoginUser)
				// registerCh <- s
				logrus.Warnln("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				logrus.Warnln("REFRESH THE TOKEN")
				logrus.Warnln("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			} else if strings.Contains(s, `"m":"du"`) { // Update on last candle
				worker2Ch <- []byte(s)
			} else if strings.Contains(s, `timescale_update`) { // First trade of a candle => means new candle
				worker2Ch <- []byte(s)
			} else if strings.Contains(s, UNSUPPORTED_RESOLUTION_ERROR) {
				continue
			} else if slices.ContainsFunc(KNOWN_STOP_ERRORS, func(err string) bool { return strings.Contains(s, err) }) {
				printCh <- s
				continue
				// closeCh <- struct{}{}
			} else if slices.ContainsFunc(KNOWN_RECONNECT_ERRORS, func(err string) bool { errStr = err; return strings.Contains(s, err) }) {
				logrus.Warnln("Received error", errStr, s)
				registerCh <- s
				// reconnectCh <- struct{}{}
			} else {
				// fmt.Println(s)
				continue
			}
		}
	}
}
func (socket *TradingViewSocket) decodeWorker(worker2Ch <-chan []byte, converterChs map[string]chan *Candle) {
	defer func() { recover() }()
	for result := range worker2Ch {
		msg, err := UnmarshalMessage(result)
		if err != nil {
			logrus.Printf("failed to unmarshal JSON segment: %v; segment: %s", err, result)
			continue
		}
		if len(msg.P.Sds.Sds1.Candles) > 0 {
			chartSession := msg.P.ChartSessionID
			if converterCh, ok := converterChs[chartSession]; ok {
				for _, s := range msg.P.Sds.Sds1.Candles {
					converterCh <- s
				}
			} else {
				logrus.Warnln("wrong chartsession", chartSession)
			}
		}
		//  else {
		// 	// logrus.Warnln("Empty Sds container", msg.P.Second)
		// }
	}
}

func (socket *TradingViewSocket) converter(symbol string, interval types.Interval, converterCh chan *Candle, klineCh chan *types.Kline) {
	defer func() { recover() }()
	index := 0
	for s := range converterCh {
		var isClosed bool
		candleData := s.V
		// if symbol == logSymbol && interval == logInterval {
		// 	fmt.Println("Income Kline", index, s.I, time.Unix(int64(candleData[0]), 0), candleData)
		// }
		if index > s.I { // old data comming late
			isClosed = true
		} else if s.I == index {
		} else {
			isClosed = false
			index = s.I
		}
		var v float64 = 0
		if len(candleData) > 5 {
			v = candleData[5]
		}
		var startTimeS int64
		if interval == types.D1 {
			startTimeS = time.Unix(int64(candleData[0]), 0).Add(24 * time.Hour).Truncate(24 * time.Hour).Unix()
		} else {
			startTimeS = int64(candleData[0]) // in seconds
		}
		startTime := startTimeS * 1000 // in milliseconds
		OpenStr := strconv.FormatFloat(candleData[1], 'f', 8, 64)
		HighStr := strconv.FormatFloat(candleData[2], 'f', 8, 64)
		LowStr := strconv.FormatFloat(candleData[3], 'f', 8, 64)
		CloseStr := strconv.FormatFloat(candleData[4], 'f', 8, 64)
		VolumeStr := strconv.FormatFloat(v, 'f', 8, 64)
		kline := &types.Kline{
			Symbol:     symbol,
			Interval:   interval,
			Time:       time.Now().UTC().Unix(),
			StartTime:  startTime,
			StartTimeS: startTimeS,
			Open:       candleData[1],
			OpenStr:    OpenStr,
			High:       candleData[2],
			HighStr:    HighStr,
			Low:        candleData[3],
			LowStr:     LowStr,
			Close:      candleData[4],
			CloseStr:   CloseStr,
			Volume:     v,
			VolumeStr:  VolumeStr,
			CloseTime:  startTime + utils.INTERVAL_MS[interval],
			CloseTimeS: startTimeS + utils.INTERVAL_S_64[interval],
			IsClosed:   isClosed,
		}
		klineCh <- kline
	}
}
*/
