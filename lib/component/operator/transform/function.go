package transform

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/log"
	"athena/lib/properties"
)

var (
	FilterProperty = properties.NewProperty("function", "", "")
)

type filter struct {
	Filter
	ctx          athena.Context
	acker        athena.ACKer
	logger       athena.Logger
	properties   athena.Properties
	operatorFunc Process

	emitNext athena.EmitNext
}

func (o *filter) Open(ctx athena.Context) error {
	o.ctx = ctx
	o.logger = log.Ctx(o.ctx)
	o.properties = ctx.Properties()
	o.properties.GetString(FilterProperty)
	return nil
}

func (o *filter) Close() error {
	return nil
}

func (o *filter) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{FilterProperty}
}

func (o *filter) Collect(emitNext athena.EmitNext) error {
	o.emitNext = emitNext
	<-o.ctx.Done()
	return nil
}
func (o *filter) GenerateEmit(_ athena.Context) athena.Emit {
	return o.emit
}

func (o *filter) emit(event *athena.Event) {
	ok := o.Filter(event)
	if ok {
		o.emitNext(event, nil)
	} else {
		o.acker.OnACK(event, false)
	}
}

func New() athena.Operator {
	return &filter{}
}
func init() {
	component.RegisterNewOperatorFunc("parse-log", New)
}
