package transform

import (
	"athena/athena"
)

//Process extend the functionality of operator,it should be lightweight, stateless
type Process func(emitNext athena.EmitNext) athena.EmitNext

type Filter func(event *athena.Event) bool

type Map func(event *athena.Event) *athena.Event

type FlatMap func(event *athena.Event, emitNext athena.EmitNext)

type NewProcessFunc func(ctx athena.Context) Process
type NewFilterFunc func(ctx athena.Context) Filter
type NewMapFunc func(ctx athena.Context) Map
type NewFlatMapFunc func(ctx athena.Context) FlatMap
