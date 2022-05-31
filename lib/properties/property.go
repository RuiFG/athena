package properties

import (
	"athena/athena"
	"reflect"
)

type property[T any] struct {
	name        string
	description string
	_default    interface{}
	_t          T
}

func (p *property[T]) Required() bool {
	return p._default == nil
}

func (p *property[T]) Name() string {
	return p.name
}

func (p *property[T]) Description() string {
	return p.description
}

func (p *property[T]) Default() interface{} {
	return p._default
}

func (p *property[T]) Type() string {
	return reflect.TypeOf(p._t).String()
}

func NewProperty[T any](name, description string, _default T) athena.Property {
	return &property[T]{
		name:        name,
		description: description,
		_default:    _default,
	}
}
func NewRequiredProperty[T any](name, description string) athena.Property {
	return &property[T]{
		name:        name,
		description: description,
	}
}
