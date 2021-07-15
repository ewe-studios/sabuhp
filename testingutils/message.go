package testingutils

import (
	"github.com/ewe-studios/sabuhp/sabu"
	"testing"

	"github.com/ewe-studios/websocket"
)

func Msg(topic sabu.Topic, message string, fromAddr string) sabu.Message {
	return sabu.Message{
		Topic:       topic,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: sabu.MessageContentType,
	}
}

func EncodedMsg(codec sabu.Codec, topic sabu.Topic, message string, fromAddr string) ([]byte, error) {
	return codec.Encode(sabu.Message{
		Topic:       topic,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: sabu.MessageContentType,
	})
}

func DecodeMsg(codec sabu.Codec, data []byte) (sabu.Message, error) {
	return codec.Decode(data)
}

func ReceiveMsg(t *testing.T, ws *websocket.Conn, codec sabu.Codec) (sabu.Message, error) {
	t.Helper()

	var m, err = GetWSMessage(t, ws)
	if err != nil {
		t.Fatalf("%v", err)
	}

	return codec.Decode(m)
}
