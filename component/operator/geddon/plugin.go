package geddon

import (
	"athena"
	"athena/component/operator/geddon/plugin"
	"athena/component/operator/geddon/plugin/nginx"
)

func newNginxBundle(ctx athena.Context, emitNext athena.EmitNext) plugin.Bundle {
	return plugin.NewNginxBundle(ctx,
		[]plugin.NginxBulkPlugin{plugin.NewNginxBulkPlugin(ctx, emitNext, &nginx.PluginSTD{})})
}

func newSquidBundle(ctx athena.Context, emitNext athena.EmitNext) plugin.Bundle {
	return plugin.NginxBundle{}
}
func newAllBundle(ctx athena.Context, emitNext athena.EmitNext) plugin.Bundle {
	return plugin.NewNginxBundle(ctx,
		[]plugin.NginxBulkPlugin{plugin.NewNginxBulkPlugin(ctx, emitNext, &nginx.PluginSTD{})})
}
