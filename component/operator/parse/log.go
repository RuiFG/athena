package parse

import (
	"athena"
	"athena/event"
	"athena/pkg/function"
	"athena/properties"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

var (
	FieldProperty = properties.NewProperty("field", "", "")
)

type logOperator struct {
	ctx        athena.Context
	properties *viper.Viper

	field          string
	interruptError bool
	emitNext       athena.EmitNext
}

func (o *logOperator) Open(ctx athena.Context) error {
	o.ctx = ctx
	o.properties = ctx.Properties()
	o.field = o.properties.GetString(FieldProperty.Name())
	return nil
}

func (o *logOperator) Close() error {
	return nil
}

func (o *logOperator) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{FieldProperty}
}

func (o *logOperator) Collect(emitNext athena.EmitNext) error {
	o.emitNext = emitNext
	<-o.ctx.Done()
	return nil
}

func (o *logOperator) Emit(ptr event.Ptr) {
	sRaw, err := cast.ToStringE(ptr.Message)
	if err != nil {
		o.ctx.Logger().WithError(err).Warnf("can't convert ptr to string.")
		return
	}
	log, err := function.ParseLog(sRaw)
	if err != nil {
		o.ctx.Logger().WithError(err).Warnf("parse %s error", sRaw)
	}
	ptr.Message = log
	o.emitNext(ptr)
}

func New() athena.Operator {
	return &logOperator{}
}
