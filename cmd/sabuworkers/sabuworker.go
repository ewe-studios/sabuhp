package sabuworkers

import (
	"context"
	"fmt"
	"github.com/ewe-studios/sabuhp/sabu"
	"log"

	"github.com/ewe-studios/sabuhp/actions"

	"github.com/ewe-studios/sabuhp/servers/serviceServer"

	"github.com/influx6/npkg/ndaemon"

	"github.com/ewe-studios/sabuhp/bus/redispub"
	redis "github.com/go-redis/redis/v8"
)

func Execute(rctx context.Context, codec sabu.Codec, redOps redis.Options) error {
	var ctx, canceler = context.WithCancel(rctx)
	ndaemon.WaiterForKillWithSignal(ndaemon.WaitForKillChan(), canceler)

	var logger sabu.GoLogImpl

	var redisBus, busErr = redispub.Stream(redispub.Config{
		Logger: logger,
		Ctx:    ctx,
		Redis:  redOps,
		Codec:  codec,
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
		return err
	}
	return nil
}
