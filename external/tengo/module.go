package tengo

import (
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

func wrapError(err error) tengo.Object {
	if err == nil {
		return tengo.TrueValue
	}
	return &tengo.Error{Value: &tengo.String{Value: err.Error()}}
}

func init() {
	stdlib.BuiltinModules["parse"] = map[string]tengo.Object{
		"parse_origin": &tengo.UserFunction{
			Name:  "parse_origin",
			Value: parseOrigin,
		},
		"parse_edge_origin": &tengo.UserFunction{
			Name:  "parse_edge_origin",
			Value: parseEdgeOrigin,
		},
	}
}
