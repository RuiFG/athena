package tengo

import (
	"athena/external/function"
	"github.com/d5/tengo/v2"
)

func parseOrigin(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 1 {
		return nil, tengo.ErrWrongNumArguments
	}
	s1, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "first",
			Expected: "string(compatible)",
			Found:    args[0].TypeName(),
		}
	}
	log, err := function.ParseOrigin(s1)
	if err != nil {
		return wrapError(err), nil
	}
	return tengo.FromInterface(log)
}

func parseEdgeOrigin(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 1 {
		return nil, tengo.ErrWrongNumArguments
	}
	s1, ok := tengo.ToString(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "first",
			Expected: "string(compatible)",
			Found:    args[0].TypeName(),
		}
	}
	log, err := function.ParseEdgeOrigin(s1)
	if err != nil {
		return wrapError(err), nil
	}
	return tengo.FromInterface(log)
}
