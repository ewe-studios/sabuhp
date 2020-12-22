# SabuHP

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/influx6/sabuhp)

Power your backend with a hybrid service layer communicating across a message bus regardless of protocol.

It exposes a your services across both a http, server-sent events and websocket endpoints, allowing varying clients to communicate across your services over a message bus.


## Protocols

SabuHP supports the following protocols for communicating with the service server (allowing both backend and frontend easily inter-related through such protocols):

- Websocket
- HTTP
- HTTP Server Sent Events


## Getting

```
go get -u github.com/influx6/sabuhp
```

## Using

Create a sample hello service

```go
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/influx6/npkg/ndaemon"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/ochestrator"
	"github.com/influx6/sabuhp/slaves"
	"github.com/influx6/sabuhp/testingutils"
)

var (
	mainLogger = &testingutils.LoggerPub{}
)

func main() {
	var logStack = njson.Log(mainLogger)
	defer njson.ReleaseLogStack(logStack)

	// worker template registry
	var workerRegistry = slaves.NewWorkerTemplateRegistry()
	workerRegistry.Register(slaves.WorkerRequest{
		ActionName:  "hello_world",
		PubSubTopic: "hello",
		WorkerCreator: func(config slaves.WorkerConfig) *slaves.WorkerGroup {
			config.Instance = slaves.ScalingInstances
			config.Behaviour = slaves.RestartAll
			config.Action = slaves.ActionFunc(func(ctx context.Context, to string, message *sabuhp.Message, t sabuhp.Transport) {
				if sendErr := t.SendToAll(&sabuhp.Message{
					ID:       nxid.New(),
					Topic:    message.FromAddr,
					FromAddr: to,
					Payload:  []byte("hello world"),
					Metadata: nil,
					Params:   nil,
				}, 5*time.Second); sendErr != nil {
					logStack.New().
						LError().
						Message("failed to send response message").
						String("error", nerror.WrapOnly(sendErr).Error()).
						End()
				}
			})
			return slaves.NewWorkGroup(config)
		},
	})

	// register for terminal kill signal
	var masterCtx, masterEnder = context.WithCancel(context.Background())
	ndaemon.WaiterForKillWithSignal(ndaemon.WaitForKillChan(), masterEnder)

	var workerId = nxid.New()
	var station = ochestrator.DefaultStation(masterCtx, workerId, ":7800", mainLogger, workerRegistry)

	// use json encoder
	station.CreateCodec = ochestrator.JsonCodec

	if stationInitErr := station.Init(); stationInitErr != nil {
		var wrapErr = nerror.WrapOnly(stationInitErr)
		log.Fatalf("Closing application due to station initialization: %+q", wrapErr)
	}

	// create a http to event route redirect to an event
	station.Router().RedirectTo("hello", "/hello")

	// create a normal http route
	station.Router().Http("/pop", sabuhp.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
		params sabuhp.Params,
	) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("stay forever!"))
	}))

	if err := station.Wait(); err != nil {
		var wrapErr = nerror.WrapOnly(err)
		log.Fatalf("Closing application: %+q", wrapErr)
	}
}

```

## Contact

Ewetumo Alexander [@influx6](http://twitter.com/influx6)

## License

Source code is available under the MIT [License](/LICENSE).
