package runtime

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/context"
	"athena/lib/emit"
	"athena/lib/log"
	"athena/lib/properties"
	"athena/lib/runtime/task"
	"athena/pkg/constant"
	_c "context"
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"
	"os"
	"os/signal"
	"syscall"
)

type QOS uint

const (
	SourcePrefix   = "source"
	OperatorPrefix = "operator"
	SinkPrefix     = "sink"
)

var (
	propertiesDef = athena.PropertiesDef{constant.RuntimeModeProperty, constant.RuntimeLogLevelProperty, constant.RuntimeStatusDirProperty}
)

type Runtime struct {
	ctx           athena.Context
	logger        athena.Logger
	runtime       athena.Properties
	life          *tomb.Tomb
	sourceTasks   map[athena.Context]*task.SourceTask
	operatorTasks map[athena.Context]*task.OperatorTask
	sinkTasks     map[athena.Context]*task.SinkTask

	allEmitNext map[athena.Context]athena.EmitGenerator
	topology    map[athena.Context][]athena.Context
}

func (e *Runtime) initSources() {
	sourceNames := e.ctx.Properties().PrefixKeys(SourcePrefix)
	if sourceNames == nil || len(sourceNames) == 0 {
		panic("source has to have at least one.")
	}
	for _, name := range sourceNames {
		sourceName := SourcePrefix + "." + name
		sourceCtx := e.ctx.Named(sourceName)
		if sourceCtx.Properties() == nil {
			panic("sources can't be nil")
		}
		source := component.NewSourceFunc(sourceCtx.Properties().GetString(constant.TypeProperty))()
		renderText, err := properties.InitAndRender(sourceCtx.Properties(), source.PropertiesDef())
		if err != nil {
			panic(errors.WithMessage(err, "failed to init source properties"))
		} else {
			e.logger.Infof("init %s:\n%s", sourceName, renderText)
		}
		sourceTask := &task.SourceTask{
			Source: source,
			Ctx:    sourceCtx,
			Name:   sourceName,
		}
		e.sourceTasks[sourceCtx] = sourceTask

	}
}

func (e *Runtime) initOperators() {
	operatorNames := e.ctx.Properties().PrefixKeys(OperatorPrefix)
	for _, name := range operatorNames {
		operatorName := OperatorPrefix + "." + name
		operatorCtx := e.ctx.Named(operatorName)
		if operatorCtx.Properties() == nil {
			panic(fmt.Sprintf("operator %s properties can't be nil.", operatorName))
		}
		operator := component.NewOperatorFunc(operatorCtx.Properties().GetString(constant.TypeProperty))()

		renderText, err := properties.InitAndRender(operatorCtx.Properties(), operator.PropertiesDef())
		if err != nil {
			panic(errors.WithMessage(err, "failed to init operator properties"))
		} else {
			e.logger.Infof("init %s:\n%s", operatorName, renderText)
		}
		operatorTask := &task.OperatorTask{
			Operator: operator,
			Ctx:      operatorCtx,
		}
		e.operatorTasks[operatorCtx] = operatorTask
		e.allEmitNext[operatorCtx] = operatorTask.GenerateEmit
	}
}

func (e *Runtime) initSinks() {
	sinkNames := e.ctx.Properties().PrefixKeys(SinkPrefix)
	if sinkNames == nil || len(sinkNames) == 0 {
		panic("sink has to have at least one.")
	}
	for _, name := range sinkNames {
		sinkName := SinkPrefix + "." + name
		sinkCtx := e.ctx.Named(sinkName)
		if sinkCtx.Properties() == nil {
			panic(fmt.Sprintf("sink %s properties can't be nil.", sinkName))
		}
		sink := component.NewSinkFunc(sinkCtx.Properties().GetString(constant.TypeProperty))()
		renderText, err := properties.InitAndRender(sinkCtx.Properties(), sink.PropertiesDef())
		if err != nil {
			panic(errors.WithMessage(err, "failed to init sink properties"))
		} else {
			e.logger.Infof("init %s:\n%s", sinkName, renderText)
		}
		sinkTask := &task.SinkTask{
			Sink: sink,
			Ctx:  sinkCtx,
		}
		e.sinkTasks[sinkCtx] = sinkTask
		e.allEmitNext[sinkCtx] = sinkTask.GenerateEmit
	}
}

func (e *Runtime) initTopology() {
	for _, operatorTask := range e.operatorTasks {
		emitNextGenerator := emit.NewEmitNextGeneratorFunc(operatorTask.Ctx.Properties().GetString(constant.SelectorProperty))()
		operatorTask.EmitNext = emitNextGenerator(operatorTask.Ctx, e.allEmitNext, e.topology)
	}
	for _, sourceTask := range e.sourceTasks {
		emitNextGenerator := emit.NewEmitNextGeneratorFunc(sourceTask.Ctx.Properties().GetString(constant.SelectorProperty))()
		sourceTask.EmitNext = emitNextGenerator(sourceTask.Ctx, e.allEmitNext, e.topology)
	}
	//check topology
}

func (e *Runtime) Run() {
	//notify system signal
	e.life.Go(func() error {
		c := make(chan os.Signal)
		signal.Notify(c)
		for {
			select {
			case s := <-c:
				switch s {
				case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT: // ctrl + c
					e.logger.Infof("notify system signal %s, done.", s)
					e.ctx.Cancel()
					return nil
				}
			case <-e.ctx.Done():
				e.logger.Warn("context done.")
				return nil
			}
		}
	})

	e.initSources()
	e.initOperators()
	e.initSinks()
	e.initTopology()
	e.runAll()
	<-e.life.Dead()
}

func (e *Runtime) runAll() {
	//starting
	for ctx, sinkTask := range e.sinkTasks {
		_task := sinkTask
		_ctx := ctx
		e.life.Go(func() error {
			e.logger.Infow("starting run sink task.", "task", _ctx.Name())
			var err error
			if err = _task.Run(); err != nil {
				e.logger.Errorw("failed run sink task.", "task", _ctx.Name(), "err", err)
			} else {
				e.logger.Infow("sink task is complete.", "task", _ctx.Name())
			}
			e.ctx.Cancel()
			return err
		})
	}

	for ctx, operatorTask := range e.operatorTasks {
		_task := operatorTask
		_ctx := ctx
		e.life.Go(func() error {
			e.logger.Infow("starting run operator task.", "task", _ctx.Name())
			var err error
			if err = _task.Run(); err != nil {
				e.logger.Errorw("failed run operator task.", "task", _ctx.Name(), "err", err)
			} else {
				e.logger.Infow("operator task is complete.", "task", _ctx.Name())
			}
			e.ctx.Cancel()
			return err
		})
	}

	for _, sourceTask := range e.sourceTasks {
		_task := sourceTask
		e.life.Go(func() error {
			e.logger.Infow("starting run source task.", "task", _task.Name)
			var err error
			if err = _task.Run(); err != nil {
				e.logger.Errorw("failed run source task.", "task", _task.Name, "err", err)
			} else {
				e.logger.Infow("operator task is complete.", "task", _task.Name)
			}
			e.ctx.Cancel()
			return err
		})
	}

}

func New(originCtx _c.Context, propertiesName string, propertiesType string, propertiesPath ...string) *Runtime {
	log.Setup(log.DefaultOptions().WithOutputEncoder(log.ConsoleOutputEncoder))
	ps := properties.New(propertiesName, propertiesType, propertiesPath...)
	ctx := context.New(originCtx, ps)
	logger := log.Ctx(ctx)
	initAndRender, err := properties.InitAndRender(ps.Global(), propertiesDef)
	if err != nil {
		panic(errors.WithMessage(err, "can't init runtime properties"))
	}
	logger.Infof("global:\n%s", initAndRender)

	life, _ := tomb.WithContext(ctx.Ctx())
	engine := &Runtime{
		logger:        logger,
		sourceTasks:   map[athena.Context]*task.SourceTask{},
		operatorTasks: map[athena.Context]*task.OperatorTask{},
		sinkTasks:     map[athena.Context]*task.SinkTask{},
		allEmitNext:   map[athena.Context]athena.EmitGenerator{},
		topology:      map[athena.Context][]athena.Context{},
		runtime:       ps.Global(),
		life:          life,
		ctx:           ctx,
	}
	return engine
}
