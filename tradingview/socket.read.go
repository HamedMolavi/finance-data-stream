package tradingview

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/sirupsen/logrus"
)

func (socket *Socket) ReadMessage() (messageType int, p []byte, err error) {
	return socket.conn.ReadMessage()
}

// func (socket *Socket) DuStream() pipeline.Stream[RawMessage] {
// 	return socket.msgCh
// }
// func (socket *Socket) ScStream() pipeline.Stream[RawCompleteMsg] {
// 	return socket.compCh
// }

func (socket *Socket) SocketIsFuckingMe(ctx context.Context) {
	socket.stardReadingOnce.Do(func() {
		readStage := pipeline.SocketSourceFactory(socket)
		errRegistererFunc := socket.errPipe.DefaultRegisterer()

		socket.categorizeStage(errRegistererFunc,
			readStage(context.TODO(), errRegistererFunc,
				pipeline.WithName("socket"), pipeline.WithID("socket"),
			),
			pipeline.WithName("categorize"), pipeline.WithID("categorize"),
		)
	})
}

func (socket *Socket) categorizeStage(eFunc pipeline.ErrorRegistererFunc, inbound pipeline.Stream[[]byte], opts ...pipeline.StageOption) {
	cfg := pipeline.CreateConfig(opts)
	errCh := make(chan *pipeline.Error, cap(inbound))
	eFunc(errCh)
	//
	pipelineMap := make(map[ChartSessionID]chan RawMessage, 100)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logrus.Warnln("manager.FilterStage Goroutine paniced:", err)
			}
		}()
		defer close(errCh)
		defer func() {
			for _, pipe := range pipelineMap {
				close(pipe)
			}
		}()
		for payload := range inbound {
			text := string(payload)
			// non-numeric last char
			re := regexp.MustCompile(`~m~[0-9]+~m~`)
			// Split the input string on the delimiter
			parts := re.Split(text, -1)
			for _, s := range parts {

				switch {
				case strings.TrimSpace(s) == "": // Filter out empty/whitespace-only parts
					continue // do nothing
				case strings.HasPrefix(s, `~h~`): // Ping messages, just response
					socket.WriteBytes([]byte(s))

				case
					// response: ~m~1428~m~{"m":"symbol_resolved","p":["cs_casjcotuguzl","sds_sym_1",{"source2":{"country":"LU","description":"Bitstamp","exchange-type":"exchange","id":"BITSTAMP","name":"Bitstamp","url":"https://www.bitstamp.net/"},"currency_code":"USD","source_id":"BITSTAMP","subsession_id":"regular","provider_id":"bitstamp","base_currency_id":"XTVCBTC","base_currency":"BTC","currency_id":"USD","format":"price","formatter":"price","pro_perm":"","volume_type":"base","measure":"price","allowed_adjustment":"none","short_description":"Bitcoin / U.S. dollar","variable_tick_size":"","name":"BTCUSD","full_name":"BITSTAMP:BTCUSD","pro_name":"BITSTAMP:BTCUSD","base_name":["BITSTAMP:BTCUSD"],"description":"Bitcoin / U.S. dollar","exchange":"Bitstamp","pricescale":1,"pointvalue":1.0,"minmov":1,"session":"24x7","session_display":"24x7","subsessions":[{"description":"Regular Trading Hours","id":"regular","private":false,"session":"24x7","session-display":"24x7"}],"type":"spot","typespecs":["crypto","defi"],"has_intraday":true,"fractional":false,"listed_exchange":"BITSTAMP","legs":["BITSTAMP:BTCUSD"],"is_tradable":true,"minmove2":0,"timezone":"Etc/UTC","aliases":[],"alternatives":[],"is_replayable":true,"has_adjustment":false,"has_extended_hours":false,"bar_source":"trade","bar_transform":"none","bar_fillgaps":false,"visible_plots_set":"ohlcv","is-tickbars-available":true,"exchange_listed_name":"Bitstamp"}],"t":1780850549,"t_ms":1780850549085}
					// strings.Contains(s, "symbol_resolved"), // creating a session and calling ResolveSymbol would automatically produce this message
					// ~m~95~m~{"m":"series_loading","p":["cs_casjcotuguzl","sds_1","s1"],"t":1780851033,"t_ms":1780851033916}
					strings.Contains(s, "series_loading"): // calling StartSeries would automatically produce this message
					sessionID, ok := findSessionID(s)
					if ok {
						session, ok := socket.GetChartSession(sessionID)
						if !ok {
							logrus.Warnln("session is received in socket but doesn't exist on sessions", sessionID)
							continue
						}
						_, ok = pipelineMap[sessionID]
						if ok {
							logrus.Warnln("detected duplicate session", session)
						}
						pipe := make(chan RawMessage, 100) // create a pipe to forward messages regarding this session to it
						pipelineMap[sessionID] = pipe      // keep the pipe local so you can send to it and close it
						select {
						case session.pipeTransferCh <- pipe: // also send the pipe over to whoever is listening to start receiving on it
						default:
							logrus.Warnln("someone should listen to session transfer or I wouldn't send the pipe over")
						}

					}

				case strings.Contains(s, `"m":"du"`), // I guess updates on candle
					strings.Contains(s, `timescale_update`): // New candle created
					sessionID, ok := findSessionID(s)
					if ok {
						pipe, ok := pipelineMap[sessionID]
						if ok {
							pipe <- RawMessage(s)
						} else {
							logrus.Warnln("got a RawMessage on socket with id", sessionID, "but it is not present in the map")
						}
					}

				case strings.Contains(s, "series_completed"): // a series got completed
					sessionID, ok := findSessionID(s)
					if ok {
						pipe, ok := pipelineMap[sessionID]
						if ok {
							close(pipe)
							delete(pipelineMap, sessionID)
						} else {
							logrus.Warnln("got a RawMessage on socket with id", sessionID, "but it is not present in the map")
						}
					}

				case strings.Contains(s, BAD_AUTH_TOKEN_ERROR):
					socket.authCh <- errors.New(BAD_AUTH_TOKEN_ERROR)
					// socket got disconnected for sure. Reconnect and don't try this token again

				case slices.ContainsFunc(KNOWN_STOP_ERRORS, func(err string) bool { return strings.Contains(s, err) }):
					errCh <- &pipeline.Error{
						Error:       errors.New(s),
						StageConfig: cfg,
						Input:       payload,
						Output:      s,
					}
				default:
					// fmt.Println(s)
				}

			}
		}
	}()
}

/*
func (socket *Socket) filterStage(eFunc pipeline.ErrorRegistererFunc, inbound pipeline.Stream[[]byte], opts ...pipeline.StageOption) {
	cfg := pipeline.CreateConfig(opts)
	errCh := make(chan *pipeline.Error, cap(inbound))
	eFunc(errCh)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logrus.Warnln("manager.FilterStage Goroutine paniced:", err)
			}
		}()
		defer close(errCh)
		defer close(socket.msgCh)
		defer close(socket.compCh)
		for payload := range inbound {
			text := string(payload)
			// non-numeric last char
			re := regexp.MustCompile(`~m~[0-9]+~m~`)
			// Split the input string on the delimiter
			parts := re.Split(text, -1)
			for _, s := range parts {

				switch {
				case strings.TrimSpace(s) == "": // Filter out empty/whitespace-only parts
					continue // do nothing
				case strings.HasPrefix(s, `~h~`): // Ping messages, just response
					socket.WriteBytes([]byte(s))
				case strings.Contains(s, `"m":"du"`), // I guess updates on candle
					strings.Contains(s, `timescale_update`): // New candle created
					socket.msgCh <- []byte(s)
				case strings.Contains(s, "series_completed"): // a series got completed
					socket.compCh <- []byte(s)

				case strings.Contains(s, BAD_AUTH_TOKEN_ERROR):
					socket.authCh <- errors.New(BAD_AUTH_TOKEN_ERROR)
					// socket got disconnected for sure. Reconnect and don't try this token again

				case slices.ContainsFunc(KNOWN_STOP_ERRORS, func(err string) bool { return strings.Contains(s, err) }):
					errCh <- &pipeline.Error{
						Error:       errors.New(s),
						StageConfig: cfg,
						Input:       payload,
						Output:      s,
					}
				default:
					// fmt.Println(s)
				}

			}
		}
	}()
}
*/
