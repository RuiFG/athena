package registry

import (
	"athena"
)

var (
	sinkMap     = map[string]athena.NewSinkFunc{}
	sourceMap   = map[string]athena.NewSourceFunc{}
	operatorMap = map[string]athena.NewOperatorFunc{}
)

func RegisterNewSinkFunc(_type string, sinkFunc athena.NewSinkFunc) {
	sinkMap[_type] = sinkFunc
}

func RegisterNewSourceFunc(_type string, sourceFunc athena.NewSourceFunc) {
	sourceMap[_type] = sourceFunc
}

func RegisterNewOperatorFunc(_type string, operatorFunc athena.NewOperatorFunc) {
	operatorMap[_type] = operatorFunc
}

func NewSourceFunc(_type string) athena.NewSourceFunc {
	return sourceMap[_type]
}

func NewOperatorFunc(_type string) athena.NewOperatorFunc {
	return operatorMap[_type]
}

func NewSinkFunc(_type string) athena.NewSinkFunc {
	return sinkMap[_type]
}

func ListSourceDef() map[string]athena.PropertyDef {
	sourceDefMap := map[string]athena.PropertyDef{}
	for name, sourceFunc := range sourceMap {
		sourceDefMap[name] = sourceFunc().PropertyDef()
	}
	return sourceDefMap
}

func ListOperatorDef() map[string]athena.PropertyDef {
	operatorDefMap := map[string]athena.PropertyDef{}
	for name, operatorFunc := range operatorMap {
		operatorDefMap[name] = operatorFunc().PropertyDef()
	}
	return operatorDefMap
}

func ListSinkDef() map[string]athena.PropertyDef {
	sinkDefMap := map[string]athena.PropertyDef{}
	for name, sinkFunc := range sinkMap {
		sinkDefMap[name] = sinkFunc().PropertyDef()
	}
	return sinkDefMap
}
