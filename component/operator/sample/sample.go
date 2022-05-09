package sample

import (
	"athena"
	"athena/event"
	"athena/properties"
	"sync/atomic"
)

var (
	RateProperty = properties.NewProperty[uint64]("rate", "", 10)
)

type operator struct {
	ctx athena.Context

	rate     uint64
	ops      uint64
	emitNext athena.EmitNext
}

func (o *operator) Open(ctx athena.Context) error {
	o.ctx = ctx
	o.rate = ctx.Properties().GetUint64(RateProperty.Name())

	return nil
}

func (o *operator) Close() error {
	return nil
}

func (o *operator) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{RateProperty}
}

func (o *operator) Collect(emitNext athena.EmitNext) error {
	o.emitNext = emitNext
	<-o.ctx.Done()
	return nil
}

func (o *operator) Emit(ptr event.Ptr) {
	if atomic.AddUint64(&o.ops, 1) == o.rate {
		atomic.AddUint64(&o.ops, -o.rate)
		o.emitNext(ptr)
	}

}

func New() athena.Operator {
	return &operator{}
}
