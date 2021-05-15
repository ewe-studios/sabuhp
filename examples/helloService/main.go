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

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/actions"
	"github.com/ewe-studios/sabuhp/ochestrator"
	"github.com/ewe-studios/sabuhp/testingutils"

	redis "github.com/go-redis/redis/v8"
)

var (
	mainLogger = &testingutils.LoggerPub{}
)

func main() {
	var logStack = njson.Log(mainLogger)

	// worker template registry
	var workerRegistry = actions.NewWorkerTemplateRegistry()
	workerRegistry.Register(actions.WorkerRequest{
		ActionName:  "hello_world",
		PubSubTopic: "hello",
		WorkerCreator: func(config actions.WorkerConfig) *actions.WorkerGroup {
			config.Instance = actions.ScalingInstances
			config.Behaviour = actions.RestartAll
			config.Action = actions.ActionFunc(func(ctx context.Context, job actions.Job) {
				var to = job.To
				var message = job.Msg
				if sendErr := job.Transport.SendToAll(&sabuhp.Message{
					Id:       nxid.New(),
					Topic:    message.FromAddr,
					FromAddr: to,
					Bytes:    []byte("hello world"),
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
			return actions.NewWorkGroup(config)
		},
	})

	// register for terminal kill signal
	var masterCtx, masterEnder = context.WithCancel(context.Background())
	ndaemon.WaiterForKillWithSignal(ndaemon.WaitForKillChan(), masterEnder)

	var workerId = nxid.New()
	var station = ochestrator.DefaultStation(masterCtx, workerId.String(), ":7800", mainLogger, workerRegistry)
	station.CreateTransport = ochestrator.RedisTransportWithOptions(redis.Options{
		Network: "tcp",
		Addr:    "localhost:7090",
	})
	// use json encoder
	station.CreateCodec = ochestrator.JsonCodec

	if stationInitErr := station.Init(); stationInitErr != nil {
		var wrapErr = nerror.WrapOnly(stationInitErr)
		log.Fatalf("Closing application due to station initialization:\n %+s", wrapErr)
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
		log.Println(wrapErr.Error())
		log.Fatalf("Closing application")
	}
}
