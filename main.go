package main

import (
	"crypto/tls"
	"github.com/ewe-studios/sabuhp/cmd/sabuclient"
	"github.com/ewe-studios/sabuhp/sabu"
	"github.com/ewe-studios/sabuhp/servers/clientServer"
	"github.com/go-redis/redis/v8"
	"github.com/influx6/npkg/nerror"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

func main(){
	var app = &cli.App{
		Name: "sabuhp",
		Usage: "Building distributed application one worker at a time",
		Commands: []*cli.Command{
			{
				Name: "client",
				Usage: "starts a sabuhp client server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name: "redis_username",
						Aliases:     nil,
						Usage:       "Username for logging into redis",
						EnvVars:     nil,
						FilePath:    "",
						Required:    false,
						Hidden:      false,
						TakesFile:   false,
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "redis_password",
						Aliases:     nil,
						Usage:       "Password for logging into redis",
						EnvVars:     nil,
						FilePath:    "",
						Required:    false,
						Hidden:      false,
						TakesFile:   false,
						DefaultText: "",
					},
					&cli.IntFlag{
						Name: "redis_db",
						Usage:       "DB integer for redis",
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "redis_addr",
						Usage:       "Address with port pointing to redis (0.0.0.0:6590)",
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "redis_tls_cert_key",
						Usage:       "Path to tls certificate key",
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "redis_tls_cert",
						Usage:       "Path to tls certificate file",
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "tls_cert_key",
						Usage:       "Path to tls certificate key",
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "tls_cert",
						Usage:       "Path to tls certificate file",
						DefaultText: "",
					},
					&cli.StringFlag{
						Name: "codec",
						Usage:       "Codec to be used by client, can be (messagepack, json, gob, flatbuffer)",
						DefaultText: "messagepack",
					},
					&cli.StringFlag{
						Name:        "addr",
						Aliases:     nil,
						Usage:       "Host for client",
						EnvVars:     []string{"CLIENT_HOST"},
						Value:       "",
						DefaultText: "0.0.0.0:9650",
						Destination: nil,
						HasBeenSet:  false,
					},
				},
				Action: func(context *cli.Context) error {
					var tlsCertificateFile = context.String("tls_cert")
					var tlsKeyFile = context.String("tls_cert_key")
					var tlsConfig *tls.Config
					if len(tlsCertificateFile) != 0 && len(tlsKeyFile) != 0 {
						var cer, err = tls.LoadX509KeyPair(tlsCertificateFile, tlsKeyFile)
						if err != nil {
							return nerror.WrapOnly(err)
						}

						tlsConfig = &tls.Config{Certificates: []tls.Certificate{cer}}
					}

					var redisTlsCertificateFile = context.String("redis_tls_cert")
					var redisTlsKeyFile = context.String("redis_tls_cert_key")
					var redisTlsConfig *tls.Config
					if len(redisTlsKeyFile) != 0 && len(redisTlsCertificateFile) != 0 {
						var cer, err = tls.LoadX509KeyPair(redisTlsCertificateFile, redisTlsKeyFile)
						if err != nil {
							return nerror.WrapOnly(err)
						}

						redisTlsConfig = &tls.Config{Certificates: []tls.Certificate{cer}}
					}

					var redisOp redis.Options
					redisOp.TLSConfig = redisTlsConfig
					redisOp.DB = context.Int("redis_db")
					redisOp.Addr = context.String("redis_addr")
					redisOp.Username = context.String("redis_username")
					redisOp.Password = context.String("redis_password")


					var clientAddr = context.String("addr")

					var codec sabu.Codec
					var codecName = context.String("codec")
					switch codecName {
					case "gob","gb", "GOB":
						codec = clientServer.DefaultJSONCodec
					case "json","JSON":
						codec = clientServer.DefaultJSONCodec
					case "MessagePack","messagepack", "msgpack", "msgpck":
						codec = clientServer.DefaultMsgPackCodec
					default:
						return nerror.New("unknown codec %q", codecName)
					}

					return sabuclient.Execute(context.Context, tlsConfig, codec, redisOp, clientAddr)
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalln(err)
	}
}

