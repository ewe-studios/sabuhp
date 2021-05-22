package main

import (
	"context"
	"log"

	"github.com/influx6/npkg/ndaemon"

	"github.com/ewe-studios/sabuhp"

	"github.com/ewe-studios/sabuhp/codecs"

	"github.com/ewe-studios/sabuhp/bus/redispub"
	"github.com/ewe-studios/sabuhp/servers/clientServer"
	redis "github.com/go-redis/redis/v8"
)

func main() {
	var ctx, waiter = ndaemon.CtxAndWaiterFor(context.Background())

	var logger sabuhp.GoLogImpl

	var messagePack = codecs.MessagePackCodec{}
	var redisBus, busErr = redispub.Stream(redispub.Config{
		Logger: logger,
		Ctx:    ctx,
		Codec:  &messagePack,
		Redis:  redis.Options{},
	})

	if busErr != nil {
		log.Fatalf("Failed to create bus connection: %q\n", busErr.Error())
	}

	var cs = clientServer.New(ctx, logger, redisBus)

	waiter.Wait()

	if err := cs.ErrGroup.Wait(); err != nil {
		log.Fatalf("service group finished with error: %+q", err.Error())
	}
}
