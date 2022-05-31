package athena

import (
	_c "context"
)

type Context interface {
	//Ctx is origin context name
	Ctx() _c.Context
	//Name is current context name
	Name() string
	Named(string) Context
	Properties() Properties

	//Store and Load is kv Storage function
	Store(key string, value interface{})
	Load(key string) (interface{}, bool)

	Done() <-chan struct{}
	Cancel()
}
