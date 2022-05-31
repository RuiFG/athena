package tengo

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/log"
	"athena/lib/properties"
	"fmt"
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"strings"
)

var (
	ConditionProperty = properties.NewRequiredProperty[string]("condition", "condition tengo script")
)

type filterOperator struct {
	ctx      athena.Context
	logger   athena.Logger
	acker    athena.ACKer
	emitNext athena.EmitNext
	compiled *tengo.Compiled
}

func (f *filterOperator) Open(ctx athena.Context) error {
	f.ctx = ctx
	f.logger = log.Ctx(f.ctx)
	f.acker = athena.NewACKer()
	conditionStr := f.ctx.Properties().GetString(ConditionProperty)
	script := tengo.NewScript([]byte(fmt.Sprintf("__res__ := (%s)", strings.TrimSpace(conditionStr))))
	if err := script.Add("event", emptyEvent); err != nil {
		f.logger.Errorw("can't add event to script.", "err", err)
		return err
	}
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	if compiled, err := script.Compile(); err != nil {
		f.logger.Errorw("can't compile script.", "err", err)
		return err
	} else {
		f.compiled = compiled
	}
	return nil
}

func (f *filterOperator) Close() error {
	f.acker.Close()
	return nil
}

func (f *filterOperator) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{ConditionProperty}
}

func (f *filterOperator) Emit(event *athena.Event) {
	tengoEvent, err := toTengoEvent(event)
	if err != nil {
		f.logger.Errorw("can't convert event to tengo type", "event", event, "err", err)
		f.acker.OnACK(event, false)
	}
	if err := f.compiled.Set("event", tengoEvent); err != nil {
		f.logger.Errorw("add event to script vm error.", "err", err)
		return
	}
	if err := f.compiled.RunContext(f.ctx.Ctx()); err != nil {
		f.logger.Errorw("run script error.", "err", err)
		return
	}
	switch tengoBool := f.compiled.Get("__res__").Value().(type) {
	case bool:
		if tengoBool {
			f.emitNext(event, nil)
		} else {
			f.logger.Debugf("filter event: %+v", event)
			f.acker.OnACK(event, false)
		}
	default:
		f.logger.Error("script return type not is bool.")
	}
}

func (f filterOperator) GenerateEmit(_ athena.Context) athena.Emit {
	return f.Emit
}

func (f *filterOperator) Collect(emitNext athena.EmitNext) error {
	f.emitNext = emitNext
	<-f.ctx.Done()
	return nil
}

func NewFilter() athena.Operator {
	return &filterOperator{logger: log.Named("operator.tengo-filter")}
}

func init() {
	component.RegisterNewOperatorFunc("tengo-filter", NewFilter)
}
