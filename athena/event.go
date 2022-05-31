package athena

import (
	"time"
)

//Event is not thread safety
type Event struct {
	Meta    map[string]any `json:"meta"`
	Message any            `json:"message"`
	Time    time.Time      `json:"time"`

	// for athena private use
	Private map[string]any `json:"-"`
}

const (
	PrivateACKHandler = "$private_ack_handler"
)

type ACKHandler func()

type ACKer interface {
	OnACK(event *Event, ok bool)
	Close()
}

type simpleACKer struct{}

func (n *simpleACKer) OnACK(event *Event, _ bool) {
	if event.Private != nil {
		if ackHandler, ok := event.Private[PrivateACKHandler]; ok {
			if handler, ok := ackHandler.(ACKHandler); ok {
				handler()
			}
		}
	}

}

func (n *simpleACKer) Close() {}

func NewACKer() ACKer {
	return &simpleACKer{}
}
