package topo

import (
	"athena"
	"athena/event"
)

type OperatorTaskOption func(task *OperatorTask)

type OperatorTask struct {
	*ComponentTask
	ctx      athena.Context
	operator athena.Operator

	emitNextGenerator EmitNextGenerator
}

func (o *OperatorTask) Emit(ptr event.Ptr) {
	o.operator.Emit(ptr)
}

func (o *OperatorTask) Starter() error {
	if err := o.operator.Open(o.ctx); err != nil {
		return err
	}

	return o.operator.Collect(o.emitNextGenerator())
}

func (o *OperatorTask) Stopper() error {
	return o.operator.Close()
}

func NewOperatorBox(ctx athena.Context, name string, operator athena.Operator, emitNextGenerator EmitNextGenerator, options ...OperatorTaskOption) *OperatorTask {
	operatorBox := &OperatorTask{
		ComponentTask:     &ComponentTask{ctx: ctx, name: name, component: operator},
		operator:          operator,
		ctx:               ctx,
		emitNextGenerator: emitNextGenerator,
	}
	operatorBox.starter = operatorBox.Starter
	operatorBox.stopper = operatorBox.Stopper
	for _, option := range options {
		option(operatorBox)
	}
	return operatorBox
}

var _ NonRootTask = &OperatorTask{}
