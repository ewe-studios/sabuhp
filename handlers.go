package sabuhp

import (
	"sync"

	"github.com/influx6/npkg/nerror"
)

type Sock struct {
	hl      sync.Mutex
	handler SocketMessageHandler
}

func NewSocketHandlers(handler SocketMessageHandler) *Sock {
	return &Sock{handler: handler}
}

func (sh *Sock) Notify(b Message, from Socket) error {
	sh.hl.Lock()
	var handler = sh.handler
	sh.hl.Unlock()
	if handler == nil {
		return nerror.New("handler was not set")
	}
	return handler(b, from)
}

func (sh *Sock) Use(handler SocketMessageHandler) {
	sh.hl.Lock()
	sh.handler = handler
	sh.hl.Unlock()
}
