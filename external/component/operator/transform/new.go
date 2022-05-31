package transform

import (
	"athena/athena"
	"athena/external/function"
	"athena/lib/component/operator/transform"
	"athena/lib/log"
	"github.com/spf13/cast"
)

var (
	newSimpleFilterFunc transform.NewFilterFunc = func(ctx athena.Context) transform.Filter {
		return func(event *athena.Event) bool {
			return false
		}
	}
	newParseLogMapFunc transform.NewMapFunc = func(ctx athena.Context) transform.Map {
		logger := log.Ctx(ctx)
		return func(event *athena.Event) *athena.Event {
			stringE, err := cast.ToStringE(event.Message)
			if err != nil {
				logger.Warnw("can't cast event message to string.", "err", err)
				return nil
			}
			parseLog, err := function.ParseOrigin(stringE)
			if err != nil {
				logger.Warnw("can't convert event message to origin log.", "err", err)
				return nil
			}
			return &athena.Event{Message: parseLog, Meta: event.Meta, Time: event.Time, Private: event.Private}
		}
	}
)

func init() {
	transform.RegisterFilter("simple", newSimpleFilterFunc)
	transform.RegisterMap("parse_origin", newParseLogMapFunc)
}
