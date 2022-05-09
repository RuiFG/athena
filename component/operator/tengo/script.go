package tengo

import (
	"athena"
	"athena/event"
	"athena/properties"
	"github.com/d5/tengo/v2"
)

var (
	ScriptProperty = properties.NewRequiredProperty[string]("script", "tengo script, use for process event.")
)

type scriptOperator struct {
	ctx      athena.Context
	emitNext athena.EmitNext
	compiled *tengo.Compiled
}

func (o *scriptOperator) Open(ctx athena.Context) error {
	o.ctx = ctx
	scriptStr := o.ctx.Properties().GetString(ScriptProperty.Name())
	script := tengo.NewScript([]byte(scriptStr))
	if err := script.Add("event", event.EmptyTengoPtr); err != nil {
		o.ctx.Logger().WithError(err).Errorf("can't add event to script.")
		return err
	}
	if compiled, err := script.Compile(); err != nil {
		o.ctx.Logger().WithError(err).Errorf("can;t compile script.")
		return err
	} else {
		o.compiled = compiled
	}
	return nil
}

func (o *scriptOperator) Close() error {
	return nil
}

func (o *scriptOperator) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{ScriptProperty}
}

func (o *scriptOperator) Emit(ptr event.Ptr) {
	if err := o.compiled.Set("event", event.ToTengoPtr(ptr)); err != nil {
		o.ctx.Logger().WithError(err).Errorf("add event to script vm error.")
		return
	}
	if err := o.compiled.RunContext(o.ctx.Ctx()); err != nil {
		o.ctx.Logger().WithError(err).Errorf("run script error.")
		return
	}
	newPtr := o.compiled.Get("event")
	switch tengoPtr := newPtr.Value().(type) {
	case *event.TengoStruct:
		o.emitNext(event.MustNewWithTime(tengoPtr.Meta.Value, tengoPtr.Message.Value, tengoPtr.Time.Value))
	default:
		o.ctx.Logger().Errorf("script return event type not is event.Ptr.")
	}
}

func (o *scriptOperator) Collect(emitNext athena.EmitNext) error {
	o.emitNext = emitNext
	<-o.ctx.Done()
	return nil
}

func NewScript() athena.Operator {
	return &scriptOperator{}
}
