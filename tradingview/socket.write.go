package tradingview

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/gorilla/websocket"
)

func (socket *Socket) WriteJSON(j interface{}) error {
	// stringify
	st, err := json.Marshal(j)
	if err != nil {
		return err
	}
	// append headers
	message := fmt.Sprintf("~m~%d~m~%s", len(st), string(st))
	// send to socket
	err = socket.conn.WriteMessage(websocket.BinaryMessage, []byte(message))
	return err
}

func (socket *Socket) WriteBytes(payload []byte) error {
	// ?
	if !bytes.HasPrefix(payload, []byte{126, 109, 126}) {
		l := len(payload)
		prefix := fmt.Sprintf("~m~%d~m~", l)
		payload = append(append(make([]byte, 0, len(prefix)+l), prefix...), payload...)
	}
	err := socket.conn.WriteMessage(websocket.BinaryMessage, []byte(payload))
	return err
	// When writerCh gets closed, we are done with the connection so close it.
	// _ = socket.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}
