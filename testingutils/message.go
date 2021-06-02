package testingutils

import (
	"testing"

	"github.com/ewe-studios/sabuhp"

	"github.com/ewe-studios/websocket"
)

func Msg(topic sabuhp.Topic, message string, fromAddr string) sabuhp.Message {
	return sabuhp.Message{
		Topic:       topic,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: sabuhp.MessageContentType,
	}
}

func EncodedMsg(codec sabuhp.Codec, topic sabuhp.Topic, message string, fromAddr string) ([]byte, error) {
	return codec.Encode(sabuhp.Message{
		Topic:       topic,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: sabuhp.MessageContentType,
	})
}

func DecodeMsg(codec sabuhp.Codec, data []byte) (sabuhp.Message, error) {
	return codec.Decode(data)
}

func ReceiveMsg(t *testing.T, ws *websocket.Conn, codec sabuhp.Codec) (sabuhp.Message, error) {
	t.Helper()

	var m, err = GetWSMessage(t, ws)
	if err != nil {
		t.Fatalf("%v", err)
	}

	return codec.Decode(m)
}
