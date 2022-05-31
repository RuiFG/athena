package window

import (
	"athena/athena"
)

type IKey interface {
	Key() string
}

type KeyGenerator func(e *athena.Event) IKey

type Processor func(ctx athena.Context, key IKey, events []*athena.Event) []*athena.Event

type Reducer func(ctx athena.Context, left *athena.Event, right *athena.Event) *athena.Event

type WaterMark *athena.Event
