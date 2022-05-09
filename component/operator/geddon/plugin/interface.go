package plugin

import (
	"athena"
	"athena/component/operator/geddon/log_format/nginx"
	"athena/event"
)

type Bundle interface {
	Add(ptr event.Ptr)
	End(id string)
}

type NginxBulkPlugin interface {
	Add(log *nginx.LogExt)
	End(id string)
}

type NginxBulkPluginNormal interface {
	NeedCalculate(ctx athena.Context, logExt *nginx.LogExt) bool
	ID(logExt *nginx.LogExt) string
	NewStruct(ptr *nginx.LogExt) interface{}
	Calculate(ctx athena.Context, ptr *nginx.LogExt, v interface{})
	NodeType() interface{}
	PluginName() string
}

type NewBundleFunc func(ctx athena.Context, emitNext athena.EmitNext) Bundle
