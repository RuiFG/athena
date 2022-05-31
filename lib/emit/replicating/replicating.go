package replicating

import (
	"athena/athena"
	"athena/lib/emit"
	"athena/lib/properties"
	"athena/pkg/constant"
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"sync/atomic"
)

var (
	OutputsProperty = properties.NewRequiredProperty[[]string]("outputs", "replicating select outputs")
	ErrEmitNextNil  = fmt.Errorf("replicating emit next can't be nil")
)

func init() {
	emit.RegisterEmitNextGeneratorFunc("replicating", func() athena.EmitNextGenerator {
		return func(ctx athena.Context, allEmitGenerator map[athena.Context]athena.EmitGenerator, topology map[athena.Context][]athena.Context) athena.EmitNext {
			var emitNextSlice []athena.Emit
			for _, emitNextRegexp := range ctx.Properties().GetStringSlice(OutputsProperty) {
				if compile, err := regexp.Compile(emitNextRegexp); err != nil {
					panic(fmt.Sprintf("output %s can't compile.", emitNextRegexp))
				} else {
					for _ctx, emitGenerator := range allEmitGenerator {
						if compile.MatchString(_ctx.Name()) {
							emitNextSlice = append(emitNextSlice, emitGenerator(ctx))
							topology[_ctx] = append(topology[_ctx], ctx)
						}
					}
				}
			}
			if emitNextSlice == nil || len(emitNextSlice) == 0 {
				panic(ErrEmitNextNil)
			}
			var emitNext athena.EmitNext

			mode := ctx.Properties().Global().GetString(constant.RuntimeModeProperty)
			switch mode {
			case athena.Snapshot:
				emitNext = func(event *athena.Event, handler athena.ACKHandler) {
					for _, emit := range emitNextSlice {
						emit(event)
					}
					if handler != nil {
						handler()
					}
				}
			case athena.ACK:
				emitNext = func(event *athena.Event, handler athena.ACKHandler) {
					var (
						time       int64 = 0
						newHandler athena.ACKHandler
					)
					if handler != nil {
						newHandler = func() {
							if atomic.AddInt64(&time, 1) == int64(len(emitNextSlice)) {
								handler()
							}
						}
						event.Private = map[string]any{athena.PrivateACKHandler: newHandler}
					}
					for _, emit := range emitNextSlice {
						emit(event)
					}

				}
			default:
				panic(errors.WithMessage(constant.ErrUnsupportedMode, mode))
			}
			return emitNext
		}
	})
}
