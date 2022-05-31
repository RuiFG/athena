package tengo

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/log"
	"athena/lib/properties"
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

var (
	ScriptProperty = properties.NewRequiredProperty[string]("script", "tengo script, use for process event.")
)

type scriptOperator struct {
	ctx      athena.Context
	logger   athena.Logger
	acker    athena.ACKer
	emitNext athena.EmitNext
	compiled *tengo.Compiled
}

func (o *scriptOperator) Open(ctx athena.Context) error {
	o.ctx = ctx
	o.logger = log.Ctx(o.ctx)
	o.acker = athena.NewACKer()
	scriptStr := o.ctx.Properties().GetString(ScriptProperty)
	script := tengo.NewScript([]byte(scriptStr))
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	if err := script.Add("event", emptyEvent); err != nil {
		o.logger.Errorf("can't add event to script:%s", err)
		return err
	}
	if compiled, err := script.Compile(); err != nil {
		o.logger.Errorf("can't compile script:%s", err)
		return err
	} else {
		o.compiled = compiled
	}
	return nil
}

func (o *scriptOperator) Close() error {
	return nil
}

func (o *scriptOperator) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{ScriptProperty}
}

func (o *scriptOperator) emit(event *athena.Event) {
	tengoEvent, err := toTengoEvent(event)
	if err != nil {
		o.logger.Errorw("can't convert event to tengo type", "event", event, "err", err)
		o.acker.OnACK(event, false)
	}
	if err := o.compiled.Set("event", tengoEvent); err != nil {
		o.logger.Errorw("add event to script vm error.", "err", err)
		return
	}
	if err := o.compiled.RunContext(o.ctx.Ctx()); err != nil {
		o.logger.Errorw("run script error.", "err", err)
		return
	}
	newEvent := o.compiled.Get("event")
	switch _newEvent := newEvent.Value().(type) {
	case *_struct:
		res := make(map[string]any)
		for key, v := range _newEvent.Meta.Value {
			res[key] = tengo.ToInterface(v)
		}
		o.emitNext(
			&athena.Event{
				Meta:    res,
				Message: tengo.ToInterface(_newEvent.Message),
				Time:    _newEvent.Time.Value,
				Private: event.Private,
			}, nil)
	default:
		o.logger.Error("script return event type not is event.Event, drop event.")
		o.acker.OnACK(event, false)
	}
}

func (o *scriptOperator) GenerateEmit(_ athena.Context) athena.Emit {
	return o.emit
}

func (o *scriptOperator) Collect(emitNext athena.EmitNext) error {
	o.emitNext = emitNext
	<-o.ctx.Done()
	return nil
}

func NewScript() athena.Operator {
	return &scriptOperator{}
}

func init() {
	component.RegisterNewOperatorFunc("tengo-script", NewScript)
}
