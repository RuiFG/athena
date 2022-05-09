package checkpoint

import (
	"athena"
	"athena/event"
)

type BarrierHandler interface {
	Process(event event.Ptr)
	SetEmit(emitNext athena.EmitNext)
}
