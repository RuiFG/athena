package checkpoint

import (
	"athena/athena"
)

type BarrierHandler interface {
	Process(event *athena.Event)
	SetEmit(emitNext athena.Emit)
}
