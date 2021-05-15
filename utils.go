package sabuhp

import (
	"github.com/influx6/npkg/njson"
	"log"
)

// SplitMessagesToGroups will split messages into subscription, unsubscription and
// message groups.
func SplitMessagesToGroups(b []Message) (subGroups []Message, unsubGroups []Message, dataGroups []Message) {
	var halfLength = len(b) / 2
	if halfLength == 0 {
		halfLength = 1
	}

	subGroups = make([]Message, 0, halfLength)
	unsubGroups = make([]Message, 0, halfLength)
	dataGroups = make([]Message, 0, halfLength)

	for _, item := range b {
		if item.Topic == UNSUBSCRIBE {
			unsubGroups = append(unsubGroups, item)
			continue
		}
		if item.Topic == SUBSCRIBE {
			subGroups = append(subGroups, item)
			continue
		}
		dataGroups = append(dataGroups, item)
	}

	return
}

type GoLogImpl struct{}

func (l GoLogImpl) Log(cb *njson.JSON) {
	log.Println(cb.Message())
	log.Println("")
}

var _ MessageBus = (*BusBuilder)(nil)

type BusBuilder struct {
	SendFunc func(data ...Message)
	ListenFunc    func(topic string, grp string, handler TransportResponse) Channel
}


func (t BusBuilder) Listen(topic string, grp string, handler TransportResponse) Channel {
	return t.ListenFunc(topic, grp, handler)
}

func (t BusBuilder) Send(data ...Message) {
	t.SendFunc(data...)
}

