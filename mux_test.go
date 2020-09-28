package sabuhp_test

import (
	"testing"

	"github.com/influx6/sabuhp"
	"github.com/stretchr/testify/require"
)

func TestMuxRouter(t *testing.T) {
	var mr = sabuhp.NewMux()
	mr.UseHandleFunc("/reply", func(request *sabuhp.Request, params sabuhp.Params) sabuhp.Response {
		require.NotNil(t, request)
		require.Equal(t, request.URL.Path, "/reply")
		require.Equal(t, request.URL.Host, "localhost:8000")
		return sabuhp.Response{Code: 200}
	})

	var req, reqErr = sabuhp.NewRequest("http://localhost:8000/reply", "GET", nil)
	require.NoError(t, reqErr)
	require.NotNil(t, req)

	var response = mr.Handle(req)
	require.Equal(t, 200, response.Code)
}
