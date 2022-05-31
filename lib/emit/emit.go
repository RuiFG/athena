package emit

import (
	"athena/athena"
)

var (
	emitNextGeneratorMap = map[string]athena.NewEmitNextGeneratorFunc{}
)

func RegisterEmitNextGeneratorFunc(name string, emitNextGeneratorFunc athena.NewEmitNextGeneratorFunc) {
	emitNextGeneratorMap[name] = emitNextGeneratorFunc
}

func NewEmitNextGeneratorFunc(name string) athena.NewEmitNextGeneratorFunc {
	return emitNextGeneratorMap[name]
}
