package component

import (
	"athena/athena"
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

func ListSourceDef() map[string]athena.PropertiesDef {
	sourceDefMap := map[string]athena.PropertiesDef{}
	for name, sourceFunc := range sourceMap {
		sourceDefMap[name] = sourceFunc().PropertiesDef()
	}
	return sourceDefMap
}

func ListOperatorDef() map[string]athena.PropertiesDef {
	operatorDefMap := map[string]athena.PropertiesDef{}
	for name, operatorFunc := range operatorMap {
		operatorDefMap[name] = operatorFunc().PropertiesDef()
	}
	return operatorDefMap
}

func ListSinkDef() map[string]athena.PropertiesDef {
	sinkDefMap := map[string]athena.PropertiesDef{}
	for name, sinkFunc := range sinkMap {
		sinkDefMap[name] = sinkFunc().PropertiesDef()
	}
	return sinkDefMap
}
