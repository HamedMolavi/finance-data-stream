package tradingview

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/HamedMolavi/finance-data-stream/types"
	"github.com/HamedMolavi/finance-data-stream/utils"
	"github.com/sirupsen/logrus"
)

type ChartSessionID string

func newChartSessionID() ChartSessionID {
	return ChartSessionID(utils.RandomFuncOptions{Prefix: "cs_", Length: 12}.GenerateRandomString())
}

/////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////

type ChartSession struct {
	socket         *Socket
	id             ChartSessionID
	timezone       string
	symbol         types.Symbol
	symbolResolved atomic.Bool
	timeframe      types.Timeframe
	limit          int
	seriesStarted  atomic.Bool
	pipeTransferCh chan (<-chan RawMessage)
}

func (session *ChartSession) ID() ChartSessionID         { return session.id }
func (session *ChartSession) Timezone() string           { return session.timezone }
func (session *ChartSession) Symbol() types.Symbol       { return session.symbol }
func (session *ChartSession) SymbolResolved() bool       { return session.symbolResolved.Load() }
func (session *ChartSession) Timeframe() types.Timeframe { return session.timeframe }
func (session *ChartSession) Limit() int                 { return session.limit }
func (session *ChartSession) SeriesStarted() bool        { return session.seriesStarted.Load() }

/////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////

type RoutingTableKey struct {
	symbol   types.Symbol
	interval types.Interval
}

func (socket *Socket) SessionStageFactory(symbol types.Symbol, interval types.Interval, Limit int) (pipeline.Stream[*types.Kline], error) {
	timeFrame, ok := types.INTERVAL_TIMEFRAME_MAP[types.FOREX_MARKET][interval]
	if !ok {
		return nil, errors.New(fmt.Sprint("creating a forex session: wrong interval", interval.String(), "; forex doesn't have equivalent timeframe."))
	}
	session, err := socket.AddChartSession(symbol, timeFrame, Limit)
	if err != nil {
		return nil, errors.New(fmt.Sprint("adding a new chartsession for", symbol, timeFrame, "failed:", err))
	}
	if err = session.SwitchTimezone("Etc/UTC"); err != nil {
		return nil, errors.New(fmt.Sprint("switching chartsession timezone for", symbol, timeFrame, "failed:", err))
	}
	if err = session.ResolveSymbol(); err != nil {
		return nil, errors.New(fmt.Sprint("resolving chartsession symbol for", symbol, timeFrame, "failed:", err))
	}
	if err = session.StartSeries(); err != nil {
		return nil, errors.New(fmt.Sprint("starting chartsession series for", symbol, timeFrame, "failed:", err))
	}
	inputPipe := <-session.pipeTransferCh
	messageStage := pipeline.MapStageFactory(func(du RawMessage) *Message {
		msg, err := UnmarshalMessage(du)
		if err != nil {
			logrus.Errorf("failed to unmarshal JSON segment: %v; segment: |%s|", err, du)
			return nil
		}
		if len(msg.P.Sds.Sds1.Candles) > 0 {
			return msg
		}
		return nil
	})
	var klineStage pipeline.ProcessStage[*Message, *types.Kline] = func(s pipeline.Stream[*Message], _ ...pipeline.StageOption) pipeline.Stream[*types.Kline] {
		outbound := make(chan *types.Kline, cap(s))
		go func() {
			defer close(outbound)

			readUpTohere := utils.LastStartTime(interval, 1) // Earliest start time we got
			for in := range s {
				firstOfSeries := float64(9000000000000)
				for _, s := range in.P.Sds.Sds1.Candles {
					candleData := s.V
					if candleData[0] > float64(readUpTohere) {
						continue
					}
					if candleData[0] < firstOfSeries {
						firstOfSeries = candleData[0]
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
					kline := &types.Kline{
						Symbol:     symbol.String(),
						Interval:   interval,
						Time:       time.Now().Unix(),
						StartTime:  startTime,
						StartTimeS: startTimeS,
						Open:       candleData[1],
						High:       candleData[2],
						Low:        candleData[3],
						Close:      candleData[4],
						Volume:     v,
						CloseTime:  startTime + types.INTERVAL_MS[interval],
						CloseTimeS: startTimeS + types.INTERVAL_S_64[interval],
						IsClosed:   true,
					}
					if kline.StartTime > 0 { //originalStartTime
						outbound <- kline
					}
				}
				if float64(readUpTohere) > firstOfSeries {
					readUpTohere = int64(firstOfSeries)
				}
			}
		}()
		return outbound
	}

	return klineStage(messageStage(inputPipe)), nil
}

/////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////

func (socket *Socket) AddChartSession(symbol types.Symbol, timeFrame types.Timeframe, limit int) (*ChartSession, error) {
	session := &ChartSession{
		pipeTransferCh: make(chan (<-chan RawMessage), 1),
		socket:         socket,
		id:             newChartSessionID(),
		timezone:       "",
		symbol:         symbol, timeframe: timeFrame, limit: limit,
	}
	err := socket.WriteJSON(SendMessage{"chart_create_session", []any{session.id, ""}})
	if err != nil {
		return session, err
	}
	socket.sessionsMu.Lock()
	defer socket.sessionsMu.Unlock()
	socket.sessions[session.id] = session
	return session, err
}
func (socket *Socket) GetChartSession(id ChartSessionID) (*ChartSession, bool) {
	socket.sessionsMu.RLock()
	defer socket.sessionsMu.RUnlock()
	session, ok := socket.sessions[id]
	return session, ok
}

func (session *ChartSession) SwitchTimezone(timezone string) error {
	if session.socket == nil {
		return errors.New("don't create a session manually. Use socket.AddChartSession handler.")
	}
	socket := session.socket
	if session.timezone != timezone {
		err := socket.WriteJSON(SendMessage{"switch_timezone", []any{session.id, timezone}})
		if err != nil {
			return err
		}
		session.timezone = timezone
	}
	return nil
}

// response: ~m~1428~m~{"m":"symbol_resolved","p":["cs_casjcotuguzl","sds_sym_1",{"source2":{"country":"LU","description":"Bitstamp","exchange-type":"exchange","id":"BITSTAMP","name":"Bitstamp","url":"https://www.bitstamp.net/"},"currency_code":"USD","source_id":"BITSTAMP","subsession_id":"regular","provider_id":"bitstamp","base_currency_id":"XTVCBTC","base_currency":"BTC","currency_id":"USD","format":"price","formatter":"price","pro_perm":"","volume_type":"base","measure":"price","allowed_adjustment":"none","short_description":"Bitcoin / U.S. dollar","variable_tick_size":"","name":"BTCUSD","full_name":"BITSTAMP:BTCUSD","pro_name":"BITSTAMP:BTCUSD","base_name":["BITSTAMP:BTCUSD"],"description":"Bitcoin / U.S. dollar","exchange":"Bitstamp","pricescale":1,"pointvalue":1.0,"minmov":1,"session":"24x7","session_display":"24x7","subsessions":[{"description":"Regular Trading Hours","id":"regular","private":false,"session":"24x7","session-display":"24x7"}],"type":"spot","typespecs":["crypto","defi"],"has_intraday":true,"fractional":false,"listed_exchange":"BITSTAMP","legs":["BITSTAMP:BTCUSD"],"is_tradable":true,"minmove2":0,"timezone":"Etc/UTC","aliases":[],"alternatives":[],"is_replayable":true,"has_adjustment":false,"has_extended_hours":false,"bar_source":"trade","bar_transform":"none","bar_fillgaps":false,"visible_plots_set":"ohlcv","is-tickbars-available":true,"exchange_listed_name":"Bitstamp"}],"t":1780850549,"t_ms":1780850549085}
func (session *ChartSession) ResolveSymbol() error {
	if session.socket == nil {
		return errors.New("don't create a session manually. Use socket.AddChartSession handler.")
	}
	socket := session.socket
	if session.symbolResolved.Load() {
		return nil // symbol already resolved
	}
	tvSession, tvExchange, tvSymbol, err := split(session.symbol, session.timeframe)
	if err != nil {
		return err
	}
	err = socket.WriteJSON(SendMessage{"resolve_symbol", []any{session.id, "sds_sym_1", fmt.Sprintf("={\"adjustment\":\"splits\",\"session\":\"%s\",\"symbol\":\"%s:%s\"}", tvSession, tvExchange, tvSymbol)}})
	if err != nil {
		return err
	}
	session.symbolResolved.Store(true)
	return nil
}

// response: ~m~95~m~{"m":"series_loading","p":["cs_casjcotuguzl","sds_1","s1"],"t":1780851033,"t_ms":1780851033916}
func (session *ChartSession) StartSeries() error {
	if session.socket == nil {
		return errors.New("don't create a session manually. Use socket.AddChartSession handler.")
	}
	socket := session.socket
	if session.seriesStarted.Load() {
		return nil // series already started
	}
	if !session.symbolResolved.Load() {
		return errors.New("you need to resolve the symbol first")
	}
	err := socket.WriteJSON(SendMessage{"create_series", []any{session.id, "sds_1", "s1", "sds_sym_1", session.timeframe, session.limit, ""}})
	if err != nil {
		return err
	}
	session.seriesStarted.Store(true)
	return nil
}

// socket.WriteBytes(fmt.Appendf(nil, `{"m":"request_more_data","p":["%s","sds_1",%d]}`, chartSession, finished))
