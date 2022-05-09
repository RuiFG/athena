package topo

import (
	"athena"
	"athena/properties"
	"fmt"
	"gopkg.in/tomb.v2"
)

// OutputsProperty TypeProperty is  component type properties
var OutputsProperty = properties.NewRequiredProperty[[]string]("outputs", "emits to")

type ComponentTask struct {
	life      tomb.Tomb
	name      string
	component athena.Component
	ctx       athena.Context
	starter   func() error
	stopper   func() error
}

func (b *ComponentTask) Name() string {
	return b.name
}

func (b *ComponentTask) Snapshot() ([]byte, error) {
	if state, ok := b.component.(athena.Stateful); ok {
		return state.Snapshot()
	} else {
		return nil, nil
	}
}

func (b *ComponentTask) Restore(snapshot []byte) error {
	if state, ok := b.component.(athena.Stateful); ok {
		return state.Restore(snapshot)
	} else {
		return nil
	}
}

func (b *ComponentTask) Start() {
	var (
		renderText string
		err        error
	)
	_, emitConfiguratorFlag := b.component.(athena.EmitConfigurator)
	_, sinkFlag := b.component.(athena.Sink)
	if sinkFlag || emitConfiguratorFlag {
		renderText, err = properties.InitPropertyDef(b.ctx, b.component.PropertyDef())
	} else {
		renderText, err = properties.InitPropertyDef(b.ctx, append(b.component.PropertyDef(), OutputsProperty))
	}
	if err != nil {
		panic(fmt.Sprintf("init component properties error:%s.", err.Error()))
	}
	b.ctx.Logger().Infof("start %s component:\n%s", b.name, renderText)
	b.life.Go(b.starter)
}

func (b *ComponentTask) Stop() {
	b.ctx.Cancel()
	<-b.life.Dead()
	err := b.stopper()
	if err != nil {
		b.ctx.Logger().WithError(err).Errorf("stop %s component error.", b.name)
	} else {
		b.ctx.Logger().Infof("stop %s component.", b.name)
	}
}
