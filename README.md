# SabuHP

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/ewe-studios/sabuhp)

Power your backend with a simple service architecture that provides direct connection into a function/processor network
through supported protocols (HTTP, WebSocket, ServerSent Events).

SabuHP exposes a two server system by providing a `Client Server` and a `Worker Server` architecture that allow 
better scaling of client connections and business logic processing in the new age of message busses as backbone of
communications.

The `client server` exists to allow direct connections from clients (CLI, Browsers) which can directly send desired 
request payload to desired topics and receive response from a target message bus. This allows us decouple the definition 
of our APIs, and their desired behaviour from how clients interact and connect to with them. The client servers purpose is
to hide way the needed intricacies to access this message queues or buses, providing a clear and familiar APIs that clients
can work with such systems with ease.

The `worker server` exists to provided scalable services that can be horizontal scaled with only required to be able to 
connect to a message bus to listen and process request payload for target topics with ease. This allows us decouple entirely
how we connect and process messages or work within a software systems.

## Protocols

SabuHP supports the following protocols for communicating with the service server (allowing both backend and frontend easily inter-related through such protocols):

- Websocket
- HTTP
- HTTP Server Sent Events


## Getting

```
go get -u github.com/ewe-studios/sabuhp
```

## Client Server

Client servers provides a server which hosts all necessary client protocols (http, websocket, server-sent event routes)
which allows clients (browsers, CLI agents) to connect into the SabuHP networks allowing these clients to deliver
requests and receive responses for their requests

```go

package main

import (
	"context"
	"log"

	"github.com/influx6/npkg/ndaemon"

	"github.com/ewe-studios/sabuhp"

	"github.com/ewe-studios/sabuhp/bus/redispub"
	"github.com/ewe-studios/sabuhp/servers/clientServer"
	redis "github.com/go-redis/redis/v8"
)

func main() {
	var ctx, canceler = context.WithCancel(context.Background())
	ndaemon.WaiterForKillWithSignal(ndaemon.WaitForKillChan(), canceler)

	var logger sabuhp.GoLogImpl

	var redisBus, busErr = redispub.Stream(redispub.Config{
		Logger: logger,
		Ctx:    ctx,
		Redis:  redis.Options{},
		Codec:  clientServer.DefaultCodec,
	})

	if busErr != nil {
		log.Fatalf("Failed to create bus connection: %q\n", busErr.Error())
	}

	var cs = clientServer.New(
		ctx,
		logger,
		redisBus,
		clientServer.WithHttpAddr("0.0.0.0:9650"),
	)

	cs.Start()

	log.Println("Starting client server")
	if err := cs.ErrGroup.Wait(); err != nil {
		log.Fatalf("service group finished with error: %+s", err.Error())
	}
}
```


## Worker Server

Worker servers exposes a server with different registered workers (Functions, Processors) who will listen to the 
connected message bus for new requests to be processed. These servers can be scaled horizontally and grouped into
listen groups based on support by the underline message bus to create a cloud of processors that allow endless scaling.


```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ewe-studios/sabuhp/actions"

	"github.com/ewe-studios/sabuhp/servers/serviceServer"

	"github.com/influx6/npkg/ndaemon"

	"github.com/ewe-studios/sabuhp"

	"github.com/ewe-studios/sabuhp/bus/redispub"
	"github.com/ewe-studios/sabuhp/servers/clientServer"
	redis "github.com/go-redis/redis/v8"
)

func main() {
	var ctx, canceler = context.WithCancel(context.Background())
	ndaemon.WaiterForKillWithSignal(ndaemon.WaitForKillChan(), canceler)

	var logger sabuhp.GoLogImpl

	var redisBus, busErr = redispub.Stream(redispub.Config{
		Logger: logger,
		Ctx:    ctx,
		Redis:  redis.Options{},
		Codec:  clientServer.DefaultCodec,
	})

	if busErr != nil {
		log.Fatalf("Failed to create bus connection: %q\n", busErr.Error())
	}

	var workers = actions.NewWorkerTemplateRegistry()
	var cs = serviceServer.New(
		ctx,
		logger,
		redisBus,
		serviceServer.WithWorkerRegistry(workers),
	)

	fmt.Println("Starting worker service")
	cs.Start()

	fmt.Println("Started worker service")
	if err := cs.ErrGroup.Wait(); err != nil {
		log.Fatalf("service group finished with error: %+s", err.Error())
	}

	fmt.Println("Closed worker service")
}

```

## Contact

Ewetumo Alexander [@influx6](http://twitter.com/influx6)

## License

Source code is available under the MIT [License](/LICENSE).
