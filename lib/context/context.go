package context

import (
	"athena/athena"
	_c "context"
	"strings"
	"sync"
)

type context struct {
	ctx    _c.Context
	v      athena.Properties
	cancel _c.CancelFunc
	kv     sync.Map
	name   string
}

func (c *context) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *context) Cancel() {
	c.cancel()
}

func (c *context) Ctx() _c.Context {
	return c.ctx
}

func (c *context) Name() string {
	return c.name
}

func (c *context) Named(value string) athena.Context {
	ctx, cancel := _c.WithCancel(c.ctx)
	name := value
	if c.name != "" {
		name = strings.Join([]string{c.name, value}, ".")
	}
	return &context{v: c.v.Sub(value), ctx: ctx, cancel: cancel, name: name}
}

func (c *context) Properties() athena.Properties {
	return c.v
}

func (c *context) Store(key string, value interface{}) {
	c.kv.Store(key, value)
}

func (c *context) Load(key string) (interface{}, bool) {
	return c.kv.Load(key)
}

func New(ctx _c.Context, properties athena.Properties) athena.Context {
	parent, cancelFunc := _c.WithCancel(ctx)
	c := &context{ctx: parent, v: properties, cancel: cancelFunc, name: ""}
	return c
}
