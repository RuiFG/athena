package plugin

import (
	"athena"
	"athena/component/operator/geddon/log_format/nginx"
	"athena/event"
	"fmt"
	"github.com/spf13/cast"
	"reflect"
	"runtime/debug"
)

func safeAddNginx(f func(ptr *nginx.LogExt), ptr *nginx.LogExt) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err.(error).Error())
			debug.PrintStack()
		}
	}()

	f(ptr)
}

type NginxBundle struct {
	ctx     athena.Context
	plugins []NginxBulkPlugin
}

func (b NginxBundle) Add(ptr event.Ptr) {
	switch raw := ptr.Message.(type) {
	case string:
		//default vendor
		vendor := "baishan"
		if vendorI, ok := ptr.Meta["vendor"]; ok {
			if vendorStr, err := cast.ToStringE(vendorI); err != nil {
				b.ctx.Logger().WithError(err).Warnf("event vendor meta type is not string.")
			} else {
				vendor = vendorStr
			}
		}
		//default node_version_type
		nodeVersionType := 0
		if nodeVersionTypeI, ok := ptr.Meta["node_version_type"]; ok {
			if nodeVersionTypeInt, err := cast.ToIntE(nodeVersionTypeI); err != nil {
				b.ctx.Logger().WithError(err).Warnf("event node version type meta type is not int.")
			} else {
				nodeVersionType = nodeVersionTypeInt
			}
		}
		if logExt, err := nginx.NewLogExt(raw, vendor, nodeVersionType); err != nil {
			b.ctx.Logger().WithError(err).Errorf("event can't convert nginx log ext.")
			return
		} else {
			for _, plugin := range b.plugins {
				safeAddNginx(plugin.Add, logExt)
			}
		}

	default:
		b.ctx.Logger().Errorf("event message type is not string.")
	}

}

func (b NginxBundle) End(id string) {
	for _, plugin := range b.plugins {
		plugin.End(id)
	}
}

type normalPlugin struct {
	ctx  athena.Context
	next athena.EmitNext
	m    map[string]interface{}
	t    reflect.Type
	NginxBulkPluginNormal
}

func (n *normalPlugin) Add(log *nginx.LogExt) {
	if !n.NeedCalculate(n.ctx, log) {
		return
	}
	id := n.ID(log)
	if n.m[id] == nil {
		v := n.NewStruct(log)
		if _t := reflect.TypeOf(v); _t != n.t {
			panic(fmt.Errorf("type not match: type registered [%s] and type NewStruct retured [%s]", n.t.String(), _t.String()))
		}
		n.m[id] = v
	}
	n.Calculate(n.ctx, log, n.m[id])
}

func (n *normalPlugin) End(fileID string) {
	if len(n.m) == 0 {
		n.next(event.MustNewPtr(map[string]interface{}{"id": n.PluginName() + "_" + fileID, "type": "json"}, reflect.Zero(reflect.SliceOf(n.t)).Interface()))
		return
	}
	result := reflect.MakeSlice(reflect.SliceOf(n.t), len(n.m), len(n.m))

	i := 0
	for _, v := range n.m {
		result.Index(i).Set(reflect.ValueOf(v))
		i++
	}
	n.next(event.MustNewPtr(map[string]interface{}{"id": n.PluginName() + "_" + fileID, "type": "json"}, result.Interface()))
}

func NewNginxBulkPlugin(ctx athena.Context, next athena.EmitNext, normal NginxBulkPluginNormal) NginxBulkPlugin {
	return &normalPlugin{
		ctx:                   ctx,
		next:                  next,
		NginxBulkPluginNormal: normal,
		m:                     map[string]any{},
		t:                     reflect.TypeOf(normal.NodeType()),
	}
}

func NewNginxBundle(ctx athena.Context, plugins []NginxBulkPlugin) NginxBundle {
	return NginxBundle{ctx, plugins}
}
