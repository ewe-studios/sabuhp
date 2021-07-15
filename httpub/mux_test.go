package httpub

import (
	"github.com/ewe-studios/sabuhp/sabu"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMuxRouter(t *testing.T) {
	var mr = NewMux()
	mr.UseHandleFunc("/reply", func(request *Request, params sabu.Params) Response {
		require.NotNil(t, request)
		require.Equal(t, request.URL.Path, "/reply")
		require.Equal(t, request.URL.Host, "localhost:8000")
		return Response{Code: 200}
	})

	var req, reqErr = NewRequest("http://localhost:8000/reply", "GET", nil)
	require.NoError(t, reqErr)
	require.NotNil(t, req)

	var response = mr.Handle(req)
	require.Equal(t, 200, response.Code)
}
