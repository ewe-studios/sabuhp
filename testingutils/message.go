package testingutils

import (
	"testing"

	"github.com/influx6/sabuhp"

	"github.com/Ewe-Studios/websocket"
)

func Msg(topic string, message string, fromAddr string) sabuhp.Message {
	return sabuhp.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func EncodedMsg(codec sabuhp.Codec, topic string, message string, fromAddr string) ([]byte, error) {
	return codec.Encode(&sabuhp.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	})
}

func ReceiveMsg(t *testing.T, ws *websocket.Conn, codec sabuhp.Codec) (*sabuhp.Message, error) {
	t.Helper()

	var m, err = GetWSMessage(t, ws)
	if err != nil {
		t.Fatalf("%v", err)
	}

	return codec.Decode(m)
}
