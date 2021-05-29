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
