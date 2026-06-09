package tradingview

import (
	"errors"
	"time"
)

// Bad authorization: 		recv ~m~45~m~{"m":"protocol_error","p":["bad auth token"]}; then disconnected
// No authorization:			send ~m~54~m~{"m":"set_auth_token","p":["unauthorized_user_token"]}
// success authorization:	nothing!
func (socket *Socket) Authorize(token string) error {
	socket.authMu.Lock()
	defer socket.authMu.Unlock()

	if socket.lastTriedToken.Load() == token {
		// already tried this token
		if socket.authorized.Load() {
			// already authorized with this token
			return nil
		}
		return errors.New("Already tried this token and failed.")
	}

	socket.lastTriedToken.Store(token)
	err := socket.WriteJSON(SendMessage{"set_auth_token", []any{token}})
	if err != nil {
		return err
	}

	select {
	case <-time.After(time.Second + 500*time.Millisecond):
		socket.authorized.Store(true)
		return nil
	case err := <-socket.authCh:
		socket.authorized.Store(false)
		return err
	}

}
