package engine

import (
	"athena"
	"athena/context"
	"athena/engine/topo"
	"athena/event"
	"athena/logger"
	"athena/properties"
	"athena/registry"
	_c "context"
	"fmt"
	"gopkg.in/tomb.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"regexp"
	"syscall"
)

type QOS uint

const (
	AtMostOnce QOS = iota
	AtLeastOnce
	ExactlyOnce
)
const (
	SourcePrefix   = "source"
	OperatorPrefix = "operator"
	SinkPrefix     = "sink"
)

var (
	// OutputsProperty TypeProperty is  component type properties
	OutputsProperty = properties.NewRequiredProperty[[]string]("outputs", "emits to")
	TypeProperty    = properties.NewRequiredProperty[string]("type", "component type")
)

type Config struct {
	QOS      `mapstructure:"qps"`
	StateDir string `mapstructure:"state-dir" `
	LogLevel string `mapstructure:"log-level" `
}
type Engine struct {
	ctx           athena.Context
	life          tomb.Tomb
	config        Config
	sourceTasks   []*topo.SourceTask
	operatorTasks []*topo.OperatorTask
	sinkTasks     []*topo.SinkTask

	emitNextMap map[string]athena.EmitNext
}

func (e *Engine) initSources() {
	sourceNames := e.ctx.Properties().GetStringMapString(SourcePrefix)
	if sourceNames == nil || len(sourceNames) == 0 {
		panic("source has to have at least one.")
	}
	for name := range sourceNames {
		sourceName := SourcePrefix + "." + name
		sourceCtx := e.ctx.With("component", sourceName)

		if sourceCtx.Properties() == nil {
			panic("sources can't be nil")
		}

		source := registry.NewSourceFunc(sourceCtx.Properties().GetString(TypeProperty.Name()))()
		sourceBox := topo.NewSourceBox(
			sourceCtx,
			sourceName,
			source,
			e.configComponentEmitNext(sourceCtx, source))
		e.sourceTasks = append(e.sourceTasks, sourceBox)
	}
}

func (e *Engine) initOperators() {
	operatorNames := e.ctx.Properties().GetStringMapString(OperatorPrefix)
	for name := range operatorNames {
		operatorName := OperatorPrefix + "." + name
		operatorCtx := e.ctx.With("component", operatorName)
		if operatorCtx.Properties() == nil {
			panic(fmt.Sprintf("operator %s properties can't be nil.", operatorName))
		}
		operator := registry.NewOperatorFunc(operatorCtx.Properties().GetString(TypeProperty.Name()))()
		operatorBox := topo.NewOperatorBox(
			operatorCtx,
			operatorName,
			operator,
			e.configComponentEmitNext(operatorCtx, operator))
		e.operatorTasks = append(e.operatorTasks, operatorBox)
		e.emitNextMap[operatorName] = operatorBox.Emit
	}
}

func (e *Engine) initSinks() {
	sinkNames := e.ctx.Properties().GetStringMapString(OperatorPrefix)
	if sinkNames == nil || len(sinkNames) == 0 {
		panic("sink has to have at least one.")
	}
	for name := range sinkNames {
		sinkName := SinkPrefix + "." + name
		sinkCtx := e.ctx.With("component", sinkName)
		if sinkCtx.Properties() == nil {
			panic(fmt.Sprintf("sink %s properties can't be nil.", sinkName))
		}
		sink := registry.NewSinkFunc(sinkCtx.Properties().GetString(TypeProperty.Name()))()
		sinkBox := topo.NewSinkBox(
			sinkCtx,
			sinkName,
			sink)
		e.sinkTasks = append(e.sinkTasks, sinkBox)
		e.emitNextMap[sinkName] = sinkBox.Emit
	}
}

func (e *Engine) Run() {
	e.ctx.Logger().Infof("run one punch man.")
	//notify system signal
	e.life.Go(func() error {
		c := make(chan os.Signal)
		signal.Notify(c)
		for {
			select {
			case s := <-c:
				switch s {
				case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT: // ctrl + c
					e.ctx.Logger().Infof("notify system signal %s, done.", s)
					return nil
				}
			case <-e.ctx.Done():
				e.ctx.Logger().Warning("ctx done.")
				return nil
			}
		}
	})
	e.initSources()
	e.initOperators()
	e.initSinks()

	e.startAll()
	<-e.life.Dead()
	e.stopAll()

}

func (e *Engine) startAll() {
	//starting
	for _, sinkBox := range e.sinkTasks {
		e.start(sinkBox)
	}

	for _, operatorBox := range e.operatorTasks {
		e.start(operatorBox)
	}

	for _, sourceBox := range e.sourceTasks {
		e.start(sourceBox)
	}

}

func (e *Engine) stopAll() {
	//stopping
	for _, sourceBox := range e.sourceTasks {
		e.stop(sourceBox)
	}
	for _, operatorBox := range e.operatorTasks {
		e.stop(operatorBox)
	}
	for _, sinkBox := range e.sinkTasks {
		e.stop(sinkBox)
	}
}

func (e *Engine) start(box topo.Task) {
	e.ctx.Logger().Infof("start restore component %s state.", box.Name())
	bytes, err := ioutil.ReadFile(path.Join(e.config.StateDir, box.Name()))
	if err != nil {
		if os.IsNotExist(err) {
			e.ctx.Logger().WithError(err).Debugf("component %s state file is not exist.", box.Name())
		} else {
			e.ctx.Logger().WithError(err).Warnf("can't read component %s state file,skip state start.", box.Name())
		}
	} else if err = box.Restore(bytes); err != nil {
		e.ctx.Logger().WithError(err).Errorf("can't recovery component %s state, skip state start.", box.Name())
	}
	box.Start()
}

func (e *Engine) stop(box topo.Task) {
	box.Stop()
	snapshot, err := box.Snapshot()
	if err != nil {
		e.ctx.Logger().WithError(err).Errorf("can't snapshot component %s state.", box.Name())
	} else if snapshot != nil {
		if err := ioutil.WriteFile(path.Join(e.config.StateDir, box.Name()), snapshot, 0644); err != nil {
			e.ctx.Logger().WithError(err).Errorf("can't write component %s state.", box.Name())
		}
	}

}

func (e *Engine) configComponentEmitNext(componentCtx athena.Context, component athena.Component) topo.EmitNextGenerator {
	return func() athena.EmitNext {
		if emitConfigurator, ok := component.(athena.EmitConfigurator); ok {
			return emitConfigurator.Config(e.emitNextMap)
		} else {
			var emitNextSlice []athena.EmitNext
			for _, emitNextRegexp := range componentCtx.Properties().GetStringSlice(OutputsProperty.Name()) {
				if compile, err := regexp.Compile(emitNextRegexp); err != nil {
					panic(fmt.Sprintf("output %s can't compile.", emitNextRegexp))
				} else {
					for name, emitNext := range e.emitNextMap {
						if compile.MatchString(name) {
							emitNextSlice = append(emitNextSlice, emitNext)
						}
					}
				}
			}
			if emitNextSlice == nil || len(emitNextSlice) == 0 {
				panic("emit next can't be nil.")
			}
			return func(ptr event.Ptr) {
				for _, emitNext := range emitNextSlice {
					emitNext(ptr)
				}
			}
		}
	}
}

func New(originCtx _c.Context, propertiesName string, propertiesType string, propertiesPath ...string) *Engine {
	ps := properties.New(propertiesName, propertiesType, propertiesPath...)
	//default engine config
	config := Config{
		QOS:      AtMostOnce,
		StateDir: "state",
		LogLevel: "info",
	}
	if err := ps.Unmarshal(&config); err != nil {
		panic(fmt.Sprintf("init engine config error:%s", err.Error()))
	}
	engine := &Engine{
		sourceTasks:   []*topo.SourceTask{},
		operatorTasks: []*topo.OperatorTask{},
		sinkTasks:     []*topo.SinkTask{},
		emitNextMap:   map[string]athena.EmitNext{},
		ctx:           context.New(originCtx, ps, logger.New(config.LogLevel)),
		config:        config}
	return engine
}
