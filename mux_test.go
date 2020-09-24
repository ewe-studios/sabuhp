package sabuhp_test

import (
	"net/url"
	"testing"

	"github.com/influx6/sabuhp"
	"github.com/stretchr/testify/require"
)

func TestMuxRouter(t *testing.T) {
	var mr = sabuhp.NewRouter()
	mr.HandleFunc("/reply", func(request *sabuhp.Request) *sabuhp.Response {
		require.NotNil(t, request)
		require.Equal(t, request.URL.Path, "/reply")
		require.Equal(t, request.URL.Host, "localhost:8000")
		return &sabuhp.Response{}
	})

	var targetURL, targetErr = url.Parse("http://localhost:8000/reply")
	require.NoError(t, targetErr)
	require.NotNil(t, targetURL)

	mr.Serve(sabuhp.NewRequest("GET", targetURL, map[string][]string{}))
}
