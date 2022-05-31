package tengo

import (
	"athena/athena"
	"athena/lib/component"
	"github.com/d5/tengo/v2/stdlib"

	"athena/lib/log"
	"athena/lib/properties"
	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cast"
	"sync"
	"time"
)

var (
	IdProperty    = properties.NewRequiredProperty[string]("id", "tengo script, use for process event for aggregate id.")
	ValueProperty = properties.NewRequiredProperty[string]("value", "tengo script, use for process event for aggregate value.")
	CronProperty  = properties.NewProperty[string]("cron", "cron expression", "@every 1m")

	waterMarkEvent = &athena.Event{
		Meta: map[string]any{"type": "watermark"},
	}
)

type aggregateOperator struct {
	ctx      athena.Context
	logger   athena.Logger
	emitNext athena.EmitNext
	acker    athena.ACKer

	idCompiled    *tengo.Compiled
	valueCompiled *tengo.Compiled

	mutex       sync.Mutex
	cron        *cron.Cron
	mPool       map[string]*tengo.Map
	ackHandlers []athena.ACKHandler
}

func (a *aggregateOperator) Open(ctx athena.Context) error {
	a.ctx = ctx
	a.logger = log.Ctx(a.ctx)
	a.acker = athena.NewACKer()
	//init intermediate state
	a.mPool = map[string]*tengo.Map{}
	a.ackHandlers = make([]athena.ACKHandler, 0)

	//got id script string and build
	idScript := tengo.NewScript([]byte(a.ctx.Properties().GetString(IdProperty)))
	idScript.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	if err := idScript.Add("event", emptyEvent); err != nil {
		return errors.WithMessage(err, "can't add event variable to id script")
	}
	if err := idScript.Add("id", &tengo.String{Value: ""}); err != nil {
		return errors.WithMessage(err, "can't add id variable to id script")
	}
	compiled, err := idScript.Compile()
	if err != nil {
		return errors.WithMessage(err, "can't compile id script")
	}
	a.idCompiled = compiled

	//got value script string and build
	valueScript := tengo.NewScript([]byte(a.ctx.Properties().GetString(ValueProperty)))
	valueScript.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	if err = valueScript.Add("event", emptyEvent); err != nil {
		return errors.WithMessage(err, "can't add event variable to value script")
	}
	if err = valueScript.Add("value", tengo.UndefinedValue); err != nil {
		return errors.WithMessage(err, "can't add value variable to value script")
	}
	compiled, err = valueScript.Compile()
	if err != nil {
		return errors.WithMessage(err, "can't compile value script")
	}
	a.valueCompiled = compiled

	//init cron
	a.cron = cron.New(cron.WithSeconds())
	_, err = a.cron.AddFunc(a.ctx.Properties().GetString(CronProperty), a.emitWatermark)
	if err != nil {
		return errors.WithMessage(err, "can't add watermark generator function to cron")
	}
	return nil
}

func (a *aggregateOperator) Close() error {
	a.cron.Stop()
	a.acker.Close()
	a.emitWatermark()
	return nil
}

func (a *aggregateOperator) emitWatermark() {
	a.emit(waterMarkEvent)
}

func (a *aggregateOperator) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{IdProperty, ValueProperty, CronProperty}
}

func (a *aggregateOperator) GenerateEmit(_ athena.Context) athena.Emit {
	return a.emit
}

func (a *aggregateOperator) emit(event *athena.Event) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if event == waterMarkEvent {
		// event
		ackHandlers := a.ackHandlers
		eventMessage := make([]any, len(a.mPool))
		var index = 0
		for _, value := range a.mPool {
			eventMessage[index] = tengo.ToInterface(value)
		}
		a.emitNext(
			&athena.Event{
				Message: eventMessage,
				Time:    time.Now(),
			}, func() {
				for _, handler := range ackHandlers {
					handler()
				}
			})

		//re init state
		a.ackHandlers = make([]athena.ACKHandler, 0)
		a.mPool = map[string]*tengo.Map{}
		return
	}
	tengoEvent, err := toTengoEvent(event)
	if err != nil {
		a.logger.Errorw("can't convert event to tengo type", "event", event, "err", err)
		a.acker.OnACK(event, false)
	}
	if err = a.idCompiled.Set("event", tengoEvent); err != nil {
		a.logger.Errorw("can't add event variable to script, discarding event.", "event", event, "err", err)
		a.acker.OnACK(event, false)
		return
	}
	if err = a.idCompiled.RunContext(a.ctx.Ctx()); err != nil {
		a.logger.Errorw("can't run id script, discarding event.", "event", event, "err", err)
		a.acker.OnACK(event, false)
		return
	}
	id, err := cast.ToStringE(a.idCompiled.Get("id").Value())
	if err != nil {
		a.logger.Error("script return event type not is string, discarding event.")
		a.acker.OnACK(event, false)
		return
	}
	var value tengo.Object
	value, ok := a.mPool[id]
	if !ok {
		value = tengo.UndefinedValue
	}
	if err = a.valueCompiled.Set("event", tengoEvent); err != nil {
		a.logger.Errorw("can't add value variable to value script, discarding event.", "err", err)
		return
	}
	if err = a.valueCompiled.Set("value", value); err != nil {
		a.logger.Errorw("can't add value variable to value script, discarding event.", "event", event, "value", value, "err", err)
		a.acker.OnACK(event, false)
		return
	}
	if err = a.valueCompiled.RunContext(a.ctx.Ctx()); err != nil {
		a.logger.Errorw("can't run value script.", "err", err)
		a.acker.OnACK(event, false)
		return
	}
	if event.Private != nil && event.Private[athena.PrivateACKHandler] != nil {
		a.ackHandlers = append(a.ackHandlers, event.Private[athena.PrivateACKHandler].(athena.ACKHandler))
	}
	a.mPool[id] = a.valueCompiled.Get("value").Object().(*tengo.Map)
}

func (a *aggregateOperator) Collect(next athena.EmitNext) error {
	a.emitNext = next
	a.cron.Start()
	<-a.ctx.Done()
	return nil
}

func NewAggregate() athena.Operator {
	return &aggregateOperator{}
}

func init() {
	component.RegisterNewOperatorFunc("tengo-aggregate", NewAggregate)
}
