package task

import (
	"athena/athena"
)

type OperatorTask struct {
	athena.Operator
	Ctx      athena.Context
	EmitNext athena.EmitNext
}

func (o *OperatorTask) Run() error {
	if err := o.Open(o.Ctx); err != nil {
		return err
	}

	if err := o.Collect(o.EmitNext); err != nil {
		return err
	}
	return o.Close()
}
