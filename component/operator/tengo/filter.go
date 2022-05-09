package tengo

import (
	"athena"
	"athena/event"
	"athena/properties"
	"fmt"
	"github.com/d5/tengo/v2"
	"strings"
)

var (
	ConditionProperty = properties.NewRequiredProperty[string]("condition", "condition tengo script")
)

type filterOperator struct {
	ctx      athena.Context
	emitNext athena.EmitNext
	compiled *tengo.Compiled
}

func (f *filterOperator) Open(ctx athena.Context) error {
	f.ctx = ctx

	conditionStr := f.ctx.Properties().GetString(ConditionProperty.Name())
	script := tengo.NewScript([]byte(fmt.Sprintf("__res__ := (%s)", strings.TrimSpace(conditionStr))))
	if err := script.Add("event", event.EmptyTengoPtr); err != nil {
		f.ctx.Logger().WithError(err).Errorf("can't add event to script.")
		return err
	}
	if compiled, err := script.Compile(); err != nil {
		f.ctx.Logger().WithError(err).Errorf("can;t compile script.")
		return err
	} else {
		f.compiled = compiled
	}
	return nil
}

func (f *filterOperator) Close() error {
	return nil
}

func (f *filterOperator) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{ConditionProperty}
}

func (f *filterOperator) Emit(ptr event.Ptr) {
	if err := f.compiled.Set("event", event.ToTengoPtr(ptr)); err != nil {
		f.ctx.Logger().WithError(err).Errorf("add event to script vm error.")
		return
	}
	if err := f.compiled.RunContext(f.ctx.Ctx()); err != nil {
		f.ctx.Logger().WithError(err).Errorf("run script error.")
		return
	}
	switch tengoBool := f.compiled.Get("__res__").Value().(type) {
	case bool:
		if tengoBool {
			f.emitNext(ptr)
		} else {
			f.ctx.Logger().Debugf("filter event: %+v", ptr)
		}
	default:
		f.ctx.Logger().Errorf("script return type not is bool.")
	}
}

func (f *filterOperator) Collect(emitNext athena.EmitNext) error {
	f.emitNext = emitNext
	<-f.ctx.Done()
	return nil
}

func NewFilter() athena.Operator {
	return &filterOperator{}
}
