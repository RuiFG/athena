package sample

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/properties"
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
	o.rate = ctx.Properties().GetUint64(RateProperty)

	return nil
}

func (o *operator) Close() error {
	return nil
}

func (o *operator) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{RateProperty}
}

func (o *operator) Collect(emitNext athena.EmitNext) error {
	o.emitNext = emitNext
	<-o.ctx.Done()
	return nil
}

func (o *operator) GenerateEmit(_ athena.Context) athena.Emit {
	return func(event *athena.Event) {
		if atomic.AddUint64(&o.ops, 1) == o.rate {
			atomic.AddUint64(&o.ops, -o.rate)
			o.emitNext(event, nil)
		}

	}
}

func New() athena.Operator {
	return &operator{}
}

func init() {
	component.RegisterNewOperatorFunc("sample", New)
}
