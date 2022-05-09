package athena

import (
	_c "context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Context interface {
	Ctx() _c.Context
	Logger() logrus.FieldLogger
	Properties() *viper.Viper

	//Store and Load is kv Storage function
	Store(key string, value interface{})
	Load(key string) (interface{}, bool)

	With(string, string) Context
	Done() <-chan struct{}
	Cancel()
}
