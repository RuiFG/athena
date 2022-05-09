package window

import (
	"athena"
	"athena/event"
)

type IKey interface {
	Key() string
}

type KeyGenerator func(e event.Ptr) IKey

type Processor func(ctx athena.Context, key IKey, ptrs event.Ptrs) event.Ptrs

type Reducer func(ctx athena.Context, left event.Ptr, right event.Ptr) event.Ptr

type WaterMark event.Ptr
