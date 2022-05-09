package topo

import (
	"athena"
)

type Task interface {
	athena.Stateful
	Name() string
	Start()
	Stop()
}

type NonRootTask interface {
	Task
	athena.EmitTarget
}

type EmitNextGenerator func() athena.EmitNext
