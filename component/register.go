package component

import (
	geddon_operator "athena/component/operator/geddon"
	"athena/component/operator/parse"
	"athena/component/operator/sample"
	"athena/component/operator/tengo"
	"athena/component/sink/echo"
	geddon_sink "athena/component/sink/geddon"
	"athena/component/source/kafka"
	"athena/component/source/mock"
	"athena/component/source/random"
	"athena/component/source/spooldir"
	"athena/registry"
	////serialize
	//_ "onepiece/event/serialize/json"
	//_ "onepiece/event/serialize/proto"
)

func registrySource() {
	registry.RegisterNewSourceFunc("mock", mock.New)
	registry.RegisterNewSourceFunc("kafka", kafka.New)
	registry.RegisterNewSourceFunc("random", random.New)
	registry.RegisterNewSourceFunc("spooldir", spooldir.New)
}

func registryOperator() {
	registry.RegisterNewOperatorFunc("parse-log", parse.New)
	registry.RegisterNewOperatorFunc("sample", sample.New)
	registry.RegisterNewOperatorFunc("geddon", geddon_operator.New)
	registry.RegisterNewOperatorFunc("script", tengo.NewScript)
	registry.RegisterNewOperatorFunc("filter", tengo.NewFilter)
}
func registrySink() {
	registry.RegisterNewSinkFunc("echo", echo.New)
	registry.RegisterNewSinkFunc("geddon", geddon_sink.New)
}

func init() {
	registrySource()
	registryOperator()
	registrySink()
}
