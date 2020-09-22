package mux_test

import (
	"net/url"
	"testing"

	"github.com/influx6/sabuhp/mux"
	"github.com/stretchr/testify/require"
)

func TestMuxRouter(t *testing.T) {
	var mr = mux.NewRouter()
	mr.HandleFunc("/reply", func(request *mux.Request) {
		require.NotNil(t, request)
		require.Equal(t, request.URL.Path, "/reply")
		require.Equal(t, request.URL.Host, "localhost:8000")
	})

	var targetURL, targetErr = url.Parse("http://localhost:8000/reply")
	require.NoError(t, targetErr)
	require.NotNil(t, targetURL)

	mr.Serve(mux.NewRequest("GET", targetURL, map[string][]string{}))
}
