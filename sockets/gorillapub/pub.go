package gorillapub

import (
	"context"
	"github.com/ewe-studios/sabuhp"
	"net/url"
	"time"
)

const retryExpansion = time.Millisecond * 100
const EndpointDialTimeout = time.Second * 3

func CreateClient(ctx context.Context, logger sabuhp.Logger, codec sabuhp.Codec, addr string, handler sabuhp.SocketMessageHandler) (*GorillaSocket, error) {
	return GorillaClient(SocketConfig{
		Info:                  &SocketInfo{
			Headers: sabuhp.Header{},
			Query:   url.Values{},
			Path:    addr,
		},
		Ctx:                   ctx,
		Logger:                logger,
		Codec:                 codec,
		MaxRetry:              5,
		RetryFn: func(last int) time.Duration {
			return time.Duration(last) + retryExpansion
		},
		Endpoint:              DefaultEndpoint(addr, EndpointDialTimeout),
		Handler:               handler,
	})
}