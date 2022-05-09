package context

import (
	"athena"
	_c "context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"sync"
)

type context struct {
	ctx    _c.Context
	v      *viper.Viper
	logger logrus.FieldLogger
	cancel _c.CancelFunc
	kv     sync.Map
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

func (c *context) With(_type string, value string) athena.Context {
	ctx, cancel := _c.WithCancel(c.ctx)
	logger := c.logger.WithField(_type, value)
	return &context{logger: logger, v: c.v.Sub(value), ctx: ctx, cancel: cancel}
}

func (c *context) Get(key string) interface{} {
	return c.v.Get(key)
}

func (c *context) Logger() logrus.FieldLogger {
	return c.logger
}

func (c *context) Properties() *viper.Viper {
	return c.v
}

func (c *context) Store(key string, value interface{}) {
	c.kv.Store(key, value)
}

func (c *context) Load(key string) (interface{}, bool) {
	return c.kv.Load(key)
}

func New(ctx _c.Context, properties *viper.Viper, logger *logrus.Logger) athena.Context {
	parent, cancelFunc := _c.WithCancel(ctx)
	c := &context{logger: logger, ctx: parent, v: properties, cancel: cancelFunc}
	return c
}
