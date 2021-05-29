package sabuhp

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"testing"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/stretchr/testify/require"
)

type jsonCodec struct{}

func (j *jsonCodec) Encode(message Message) ([]byte, error) {
	encoded, encodedErr := json.Marshal(message)
	if encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}

	return encoded, nil
}

type LoggerPub struct{}

func (l LoggerPub) Log(cb *njson.JSON) {
	log.Println(cb.Message())
	log.Println("")
}

func (j *jsonCodec) Decode(b []byte) (Message, error) {
	var message Message
	if jsonErr := json.Unmarshal(b, &message); jsonErr != nil {
		return message, nerror.WrapOnly(jsonErr)
	}
	return message, nil
}

func TestHttpCodec(t *testing.T) {
	var logger = new(LoggerPub)
	var codec = &jsonCodec{}
	var decoder = NewHttpDecoderImpl(codec, logger, -1)
	var salesRequest, salesReqErr = http.NewRequest("GET", "/sales", bytes.NewBuffer([]byte("alex")))
	require.NoError(t, salesReqErr)
	require.NotNil(t, salesRequest)

	salesRequest.Header.Set("Content-Type", "plain/html")

	var salesRequestMessage, salesRequestMessageErr = decoder.Decode(salesRequest, Params{})
	require.NoError(t, salesRequestMessageErr)
	require.NotNil(t, salesRequestMessage)

	require.Equal(t, "/sales", salesRequestMessage.Topic)
	require.Equal(t, "/sales", salesRequestMessage.Path)
	require.Equal(t, "plain/html", salesRequestMessage.ContentType)
	require.Equal(t, "alex", string(salesRequestMessage.Bytes))
}
