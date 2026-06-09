package tradingview

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HamedMolavi/finance-data-stream/pipeline"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var bearer = make(chan struct{}, 5) // only 5 parallel connection at a time

type Socket struct {
	conn *websocket.Conn

	lastTriedToken atomic.Value
	authorized     atomic.Bool
	authMu         sync.Mutex
	authCh         chan error

	stardReadingOnce sync.Once

	errPipe *pipeline.ErrorPipeline

	sessions   map[ChartSessionID]*ChartSession
	sessionsMu sync.RWMutex
}

func NewSocket() *Socket {
	dialer := websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: true, // request permessage-deflate extension the proper way
	}
	headers := http.Header{}
	headers.Set("Host", "data.tradingview.com")
	headers.Set("Origin", "https,//data.tradingview.com")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36")
	headers.Set("Pragma", "no-cache")
	i := 1
	for {
		bearer <- struct{}{}
		conn, resp, err := dialer.Dial(WS_URL, headers)
		<-bearer
		if err == nil {
			////////////////////////////////////////////////////////////////////
			return &Socket{
				conn:    conn,
				authCh:  make(chan error, 1),
				errPipe: NewErrorPipeline(),
			}
			////////////////////////////////////////////////////////////////////
		}
		// try to read resp body if available for debugging
		var respBody string
		if resp != nil {
			if b, rerr := io.ReadAll(io.LimitReader(resp.Body, 1024)); rerr == nil {
				respBody = string(b)
			}
		}
		logrus.Warnln("Trading view websocket dial failed, retrying", err, respBody)

		// exponential backoff + jitter
		sleep := time.Second * time.Duration(1<<uint(i-1))
		i = min(i+1, 6)
		if sleep > 10*time.Second {
			sleep = 10 * time.Second
		}
		time.Sleep(sleep)
	}
}
