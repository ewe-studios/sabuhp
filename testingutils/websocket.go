package testingutils

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Ewe-Studios/websocket"
)

func HttpToWS(t *testing.T, u string) string {
	t.Helper()

	wsURL, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}

	switch wsURL.Scheme {
	case "http":
		wsURL.Scheme = "ws"
	case "https":
		wsURL.Scheme = "wss"
	}

	return wsURL.String()
}

func NewWSServer(t *testing.T, h http.Handler) (*httptest.Server, *websocket.Conn) {
	t.Helper()

	s := httptest.NewServer(h)
	wsURL := HttpToWS(t, s.URL)

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}

	return s, ws
}

func SendMessage(t *testing.T, ws *websocket.Conn, msg []byte) {
	t.Helper()
	if err := ws.WriteMessage(websocket.BinaryMessage, msg); err != nil {
		t.Fatalf("%v", err)
	}
}

func ReceiveWSMessage(t *testing.T, ws *websocket.Conn) []byte {
	t.Helper()

	var m, err = GetWSMessage(t, ws)
	if err != nil {
		t.Fatalf("%v", err)
	}

	return m
}

func GetWSMessage(t *testing.T, ws *websocket.Conn) ([]byte, error) {
	t.Helper()

	_, m, err := ws.ReadMessage()
	if err != nil {
		return nil, err
	}

	return m, nil
}
