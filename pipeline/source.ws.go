package pipeline

import (
	"context"
	"strings"

	"github.com/gorilla/websocket"
)

/*
func SocketSourceFactoryWithReconnect(socket *websocket.Conn, reconnectPortID PortID) PortSourceStage[[]byte] {
	return func(ctx context.Context, opts ...StageOption) *Ports[[]byte] {
		// Read loop: blocks until error or ctx cancelled
		payloadCh, reconnectCh := make(chan []byte, 100), make(chan struct{})
		ports := &Ports[[]byte]{defaultStream: payloadCh, values: map[PortID]any{reconnectPortID: reconnectCh}}
		go func() {
			defer func() { recover() }()
			defer close(payloadCh)
			for {
				_, payload, err := socket.ReadMessage()
				if err != nil {
					if _, ok := err.(*websocket.CloseError); ok || strings.Contains(err.Error(), "use of closed network connection") {
						return // no reconnection, closed by program itself
					}
					// Read error or closed — break to reconnect
					logrus.Warnf("read error: %s — should reconnect", err.Error())
					close(reconnectCh)
					return
				}
				payloadCh <- payload
			}
		}()
		return ports
	}
}
*/

type SocketReader interface {
	ReadMessage() (messageType int, p []byte, err error)
}

func SocketSourceFactory(socket SocketReader) TrySourceStage[[]byte] {
	// Read loop: blocks until error or ctx cancelled
	return func(ctx context.Context, eFunc ErrorRegistererFunc, opts ...StageOption) Stream[[]byte] {
		cfg := CreateConfig(opts)
		payloadCh, errCh := make(chan []byte, 100), make(chan *Error, 100)
		eFunc(errCh)
		go func() {
			defer func() { recover() }()
			defer close(payloadCh)
			defer close(errCh)
			for {
				_, payload, err := socket.ReadMessage()
				if err != nil {
					if _, ok := err.(*websocket.CloseError); ok || strings.Contains(err.Error(), "use of closed network connection") {
						errCh <- &Error{
							Error:       err,
							StageConfig: cfg,
							Input:       nil,
							Output:      payload,
						}
						return // no reconnection, closed by program itself
					}
					// Read error or closed — break to reconnect
					// logrus.Warnf("read error: %s — should reconnect", err.Error())
					errCh <- &Error{
						Error:       err,
						StageConfig: cfg,
						Input:       nil,
						Output:      payload,
					}
					return
				}
				payloadCh <- payload
			}
		}()
		return payloadCh
	}
}
