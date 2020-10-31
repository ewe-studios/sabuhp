package testingutils

import (
	"testing"

	"github.com/Ewe-Studios/websocket"
	"github.com/influx6/sabuhp/supabaiza"
)

func Msg(topic string, message string, fromAddr string) supabaiza.Message {
	return supabaiza.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func EncodedMsg(codec supabaiza.Codec, topic string, message string, fromAddr string) ([]byte, error) {
	return codec.Encode(&supabaiza.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	})
}

func ReceiveMsg(t *testing.T, ws *websocket.Conn, codec supabaiza.Codec) (*supabaiza.Message, error) {
	t.Helper()

	var m, err = GetWSMessage(t, ws)
	if err != nil {
		t.Fatalf("%v", err)
	}

	return codec.Decode(m)
}
