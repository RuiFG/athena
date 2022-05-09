package athena

import (
	"athena/event"
)

//EmitNext describe source->operator|operator->operator|operator->sink transmit data function
type EmitNext func(ptr event.Ptr)

//EmitNextWrapper extend the functionality of emit, like a filter,it should be lightweight, stateless
type EmitNextWrapper func(ctx Context, emitNext EmitNext) EmitNext

//EmitTarget is target's emit life cyc
type EmitTarget interface {
	Emit(ptr event.Ptr)
}

//Component is core
type Component interface {
	//Open initialize the component
	Open(ctx Context) error
	//Close cleaning up after the context done.
	Close() error
	//PropertyDef return Component properties defend
	PropertyDef() PropertyDef
}

type Source interface {
	Component
	//Collect should block caller,and wait for ctx done.
	Collect(EmitNext) error
}

type Operator interface {
	Component
	EmitTarget
	Collect(EmitNext) error
}

type Sink interface {
	Component
	EmitTarget
}

type NewSourceFunc func() Source
type NewSinkFunc func() Sink
type NewOperatorFunc func() Operator
