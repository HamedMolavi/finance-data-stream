package tradingview

/*
import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/danialdark/binancego/types"
	"github.com/danialdark/binancego/utils"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

func handleTvSocket(request *types.SourceRequest, originalRequest *types.Input, emitterChFn func(interval types.Interval) chan<- *types.Kline) (int64, error) {
	helper := func(symbols []string, intervals []types.Interval, reconnectCh chan struct{}, doneCh chan int, errorCh chan string, writerCh, workerCh, worker2Ch chan []byte, parentCompleteCh chan string) (int, map[string]chan *tradingview.Message) {
		total := 0
		// fmt.Println("Connecting", len(symbols), symbols)
		socket := tradingview.ConnectWithInfiniteRetry(request.URL)
		proxyChs := make(map[string]chan *tradingview.Message, len(utils.INTERVALS)*len(symbols))
		proxyForceStopChs := make(map[string]chan struct{}, len(utils.INTERVALS)*len(symbols))
		completeChs := make(map[string]chan chan int, len(utils.INTERVALS)*len(symbols))
		//////////////////////////////////////////////////////////
		for _, symbol := range symbols {
			for _, interval := range intervals {
				////////////////
				timeFrame, ok := utils.INTERVAL_TIMEFRAME_MAP[utils.FOREX_MARKET][interval]
				if !ok {
					// logrus.Warnln("no interval to timeframe conversion", interval)
					continue
				}
				////////////////
				chartSession := tradingview.GenerateRandomString("cs_", 12)
				proxyCh := make(chan *tradingview.Message, 10)
				proxyForceStopCh := make(chan struct{}, 10)
				proxyChs[chartSession] = proxyCh
				proxyForceStopChs[chartSession] = proxyForceStopCh
				completeCh := make(chan chan int)
				completeChs[chartSession] = completeCh
				copiedInterval := interval
				copiedsymbol := string([]byte(symbol))
				start, _ := utils.InputToTimeFromIn(originalRequest)
				go Proxy(socket, copiedsymbol, copiedInterval, request.Limit, start.UnixMilli(), proxyCh, proxyForceStopCh, completeCh, emitterChFn, doneCh)
				////////////////
				for _, message := range tradingview.SetupHistoryMessages(timeFrame, symbol, chartSession, request.Limit) {
					err := WriteMessage(socket, message)
					if err != nil {
						logrus.Errorln("ERROR: sent setup message", err)
					}
				}
				////////////////
				total++
				// fmt.Println(total, "Registered", symbol, interval, chartSession)
			}
		}
		//////////////////////////////////////////////////////////
		go Writer(socket, writerCh)
		go Reader(socket, writerCh, workerCh, reconnectCh)
		go ErrorHandler(socket, errorCh, proxyForceStopChs)
		go Worker(socket, writerCh, workerCh, worker2Ch, errorCh, parentCompleteCh)
		go Worker2(socket, writerCh, worker2Ch, proxyChs, completeChs, parentCompleteCh)
		return total, proxyChs
	}
	// registerCh := make(chan tradingview.SymbolIntervalTuple, 100)
	doneCh := make(chan int, 100)
	reconnectCh := make(chan struct{})
	errorCh := make(chan string, 100)
	workerCh := make(chan []byte, 1000)
	worker2Ch := make(chan []byte, 1000)
	writerCh := make(chan []byte, 100)
	parentCompleteCh := make(chan string)
	total, proxyChs := helper(request.Symbols, request.Intervals, reconnectCh, doneCh, errorCh, writerCh, workerCh, worker2Ch, parentCompleteCh)
	emitted := 0
	// then wait for reconnection
	if total != 0 {
	loop:
		for {
			// if utils.C.App() == "history" {
			// 	logrus.Infoln("Waiting for done signals", total)
			// }
			select {
			// TODO
			// case symInterval := <-registerCh:
			// 	timeFrame := utils.INTERVAL_TIMEFRAME_MAP[utils.FOREX_MARKET][symInterval.I]
			// 	chartSession := tradingview.GenerateRandomString("cs_", 12)
			// 	for _, message := range tradingview.SetupHistoryMessages(timeFrame, symInterval.S, chartSession) {
			// 		st, err := json.Marshal(message)
			// 		if err != nil {
			// 			continue
			// 		}
			// 		// append headers
			// 		s := fmt.Appendf(nil, "~m~%d~m~%s", len(st), string(st))
			// 		writerCh <- s
			// 	}
			// 	socket.chartSessionMap[chartSession] = symInterval
			case <-reconnectCh:
			case howMany := <-doneCh:
				emitted += howMany
				total--
				if total == 0 {
					close(writerCh) // will cause Writer to close the connection
					close(errorCh)
					for _, proxyCh := range proxyChs {
						close(proxyCh)
					}
					close(workerCh)
					close(worker2Ch)
					break loop
				}
				// logrus.Infoln("One time frame symbol done; remaining", total)
				continue loop
			}
			close(writerCh) // will cause Writer to close the connection
			// Reader already closed
			close(workerCh)
			close(worker2Ch)
			for _, proxyCh := range proxyChs {
				close(proxyCh)
			}
			close(errorCh)
			// registerCh = make(chan tradingview.SymbolIntervalTuple, 100)
			doneCh = make(chan int, 250)
			reconnectCh = make(chan struct{})
			errorCh = make(chan string, 250)
			workerCh = make(chan []byte, 10)
			worker2Ch = make(chan []byte, 10)
			writerCh = make(chan []byte)
			parentCompleteCh = make(chan string)
			total, proxyChs = helper(request.Symbols, request.Intervals, reconnectCh, doneCh, errorCh, writerCh, workerCh, worker2Ch, parentCompleteCh)
			emitted = 0
		}
		if utils.C.App() == "history" {
			logrus.Infoln("Ended socket request", request.Symbols, request.Intervals, emitted)
		}
	}
	return int64(emitted), nil
}

func WriteMessage(socket *websocket.Conn, j any) error {
	// stringify
	st, err := json.Marshal(j)
	if err != nil {
		return err
	}
	// append headers
	message := fmt.Sprintf("~m~%d~m~%s", len(st), string(st))
	// send to socket
	err = socket.WriteMessage(websocket.BinaryMessage, []byte(message))
	return err
}

func Writer(socket *websocket.Conn, writerCh chan []byte) {
	for payload := range writerCh {
		if !bytes.HasPrefix(payload, []byte{126, 109, 126}) {
			l := len(payload)
			prefix := fmt.Sprintf("~m~%d~m~", l)
			payload = append(append(make([]byte, 0, len(prefix)+l), prefix...), payload...)
		}
		if err := socket.WriteMessage(websocket.BinaryMessage, []byte(payload)); err != nil {
			logrus.Warnf("write error replying with digit %q: %v — will reconnect", payload, err)
			continue
		}
	}
	// When writerCh gets closed, we are done with the connection so close it.
	_ = socket.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

func Reader(socket *websocket.Conn, writerCh, workerCh chan []byte, reconnectCh chan struct{}) {
	// Read loop: blocks until error or ctx cancelled
	defer func() { recover() }()
	for {
		_, payload, err := socket.ReadMessage()
		if err != nil {
			if _, ok := err.(*websocket.CloseError); ok || strings.Contains(err.Error(), "use of closed network connection") {
				return // no reconnection, closed by program itself
			}
			// Read error or closed — break to reconnect
			logrus.Warnf("read error: %s — should reconnect", err.Error())
			reconnectCh <- struct{}{}
			return
		}
		workerCh <- payload
	}
}

func Worker(socket *websocket.Conn, writerCh, workerCh, worker2Ch chan []byte, errorCh, parentCompleteCh chan string) {
	defer func() { recover() }()
	for payload := range workerCh {
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
				// fmt.Println("Worker detected ping", s)
				writerCh <- []byte(s)
			} else if strings.Contains(s, `"m":"du"`) { // I guess updates on candles
				worker2Ch <- []byte(s)
			} else if strings.Contains(s, `timescale_update`) { // New candle created!
				worker2Ch <- []byte(s)
			} else if slices.ContainsFunc(tradingview.KNOWN_STOP_ERRORS, func(err string) bool { return strings.Contains(s, err) }) {
				errorCh <- s
			} else if strings.Contains(s, "series_completed") {
				// fmt.Println(s)
				parentCompleteCh <- s
			} else {
				// fmt.Println(s)
			}
		}
		// fmt.Println("Worker waiting...")
	}
}
func Worker2(socket *websocket.Conn, writerCh, worker2Ch chan []byte, proxyChs map[string]chan *tradingview.Message, completeChs map[string]chan chan int, parentCompleteCh chan string) {
	defer func() { recover() }()
	regex := regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)
	for {
		select {
		case s, ok := <-worker2Ch:
			if !ok {
				return
			}
			msg, err := tradingview.UnmarshalMessage(s)
			if err != nil {
				logrus.Errorf("failed to unmarshal JSON segment: %v; segment: |%s|", err, s)
				continue
			}
			if len(msg.P.Second.Sds1.S) > 0 {
				chartSession := msg.P.First
				if proxyCh, ok := proxyChs[chartSession]; ok {
					proxyCh <- msg
				} else {
					logrus.Warnln("wrong chartsession in worker", chartSession)
				}
			}
		case complete := <-parentCompleteCh:
			// fmt.Println("complete message came", complete)
			result := regex.FindStringSubmatch(string(complete))
			if len(result) > 1 {
				chartSession := result[1]
				// fmt.Println("Worker detected series completion", chartSession)
				if completeCh, ok := completeChs[chartSession]; ok {
					reply := make(chan int)
					completeCh <- reply
					finished := <-reply
					if finished > 0 {
						// fmt.Println("Worker 2 realized history hasn't finished yet", chartSession, finished)
						writerCh <- fmt.Appendf(nil, `{"m":"request_more_data","p":["%s","sds_1",%d]}`, chartSession, finished)
					} else {
						// return
					}
				}
			} else {
				logrus.Warnln("complete message came but didn't have a chart session id", complete)
			}
		}
	}
}

func Proxy(socket *websocket.Conn, symbol string, interval types.Interval, limit int, originalStartTime int64, proxyCh chan *tradingview.Message, proxyForceStopCh chan struct{}, completeCh chan chan int, emitterChFn func(interval types.Interval) chan<- *types.Kline, doneCh chan int) {
	threashold := 100
	consuming := true
	counter := 0
	m := int(limit/9000) + 5
	timeOut := time.NewTimer(time.Duration(m) * time.Minute)

	// timeOut := time.NewTimer(30 * time.Second)
	readUpTohere := utils.LastStartTime(interval, 1)
	for {
		select {
		case reply := <-completeCh:
			// fmt.Println("Proxy got series complete", symbol, interval, limit, counter)
			if consuming {
				if counter < limit && threashold > 0 {
					reply <- limit - counter
					threashold--
				} else {
					consuming = false
					reply <- 0
					doneCh <- counter
				}
			}
		case <-timeOut.C:
			if consuming {
				if utils.C.App() == "history" {
					logrus.Warnln("Timeout", symbol, interval, counter)
				}
				doneCh <- counter
				consuming = false // Since socket will continue sending updates on this interval/symbol don't consume it anymore
			}
		case <-proxyForceStopCh:
			if consuming {
				logrus.Warnln("Force done", symbol, interval, counter)
				doneCh <- counter
				consuming = false // Since socket will continue sending updates on this interval/symbol don't consume it anymore
			}
		case msg, ok := <-proxyCh:
			if !ok {
				return
			}
			if consuming {
				firstOfSeries := float64(9000000000000)
				for _, s := range msg.P.Second.Sds1.S {
					candleData := s.V
					if candleData[0] > float64(readUpTohere) {
						continue
					}
					if candleData[0] < firstOfSeries {
						firstOfSeries = candleData[0]
					}
					counter++
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
					kline := &types.Kline{
						Symbol:     symbol,
						Interval:   interval,
						Time:       time.Now().Unix(),
						StartTime:  startTime,
						StartTimeS: startTimeS,
						Open:       candleData[1],
						High:       candleData[2],
						Low:        candleData[3],
						Close:      candleData[4],
						Volume:     v,
						CloseTime:  startTime + utils.INTERVAL_MS[interval],
						CloseTimeS: startTimeS + utils.INTERVAL_S_64[interval],
						// y: oldData[exchange + "_" + symbolName] ? oldData[exchange + "_" + symbolName].y : 0,
						IsClosed: true,
					}
					// if utils.C.App() == "history" {
					// logrus.Infoln(counter, kline.Symbol, kline.Interval, int64(candleData[0]))
					// }
					//TODO
					if kline.StartTime > originalStartTime {
						emitterChFn(interval) <- kline
					}
				}
				if float64(readUpTohere) > firstOfSeries {
					readUpTohere = int64(firstOfSeries)
					// if utils.C.App() == "history" {
					// 	logrus.Infoln(len(msg.P.Second.Sds1.S), "One series sweep", time.Unix(readUpTohere, 0), "Total:", counter)
					// }
				}
			}
		}
	}
}

var chartSessionRe = regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)

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
