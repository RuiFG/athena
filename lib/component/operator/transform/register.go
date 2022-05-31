package transform

import (
	"athena/athena"
)

var (
	filterMap = map[string]NewFilterFunc{}
	mapMap    = map[string]NewMapFunc{}
)

func RegisterProcess() {

}

func RegisterFilter(name string, newFilterFunc NewFilterFunc) {
	filterMap[name] = newFilterFunc
}
func RegisterMap(name string, mapFunc NewMapFunc) {
	mapMap[name] = mapFunc
}

func newFilterFunc(ctx athena.Context, name string) Filter {
	return filterMap[name](ctx)
}
