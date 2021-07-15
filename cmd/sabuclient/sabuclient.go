package sabuclient

import (
	"context"
	"crypto/tls"
	"github.com/ewe-studios/sabuhp/sabu"
	"log"

	"github.com/influx6/npkg/ndaemon"

	"github.com/ewe-studios/sabuhp/bus/redispub"
	"github.com/ewe-studios/sabuhp/servers/clientServer"
	redis "github.com/go-redis/redis/v8"
)

func Execute(rctx context.Context, conf *tls.Config, codec sabu.Codec, redOps redis.Options, clientAddr string) error {
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
		return busErr
	}

	redisBus.Start()

	var cs = clientServer.New(
		ctx,
		logger,
		redisBus,
		clientServer.WithTLS(conf),
		clientServer.WithCodec(codec),
		clientServer.WithHttpAddr(clientAddr),
	)

	cs.Start()

	log.Println("Starting client server")
	if err := cs.ErrGroup.Wait(); err != nil {
		return err
	}
	return nil
}
