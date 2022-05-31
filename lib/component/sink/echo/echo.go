package echo

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/log"
	"athena/lib/properties"
	"athena/pkg/queue"
	"sync"
)

var (
	BatchSizeProperty = properties.NewProperty[int]("batch", "echo sink echo batch size", 100)
	TypeProperty      = properties.NewProperty[string]("echo", "echo type, like info debug", "info")
)

type sink struct {
	ctx       athena.Context
	logger    athena.Logger
	acker     athena.ACKer
	batch     int
	buffer    *queue.Queue
	bufferMux sync.Mutex
	echoFunc  func(format string, args ...interface{})
}

func (s *sink) GenerateEmit(_ athena.Context) athena.Emit {
	return func(event *athena.Event) {
		s.bufferMux.Lock()
		defer s.bufferMux.Unlock()
		s.buffer.Add(event)
		if s.buffer.Length() >= s.batch {
			for i := 0; i < s.batch; i++ {
				_event := s.buffer.Remove().(*athena.Event)
				s.echoFunc("%+v", _event)
				s.acker.OnACK(_event, true)
			}
		}
	}
}

func (s *sink) Open(ctx athena.Context) error {
	s.ctx = ctx
	s.logger = log.Ctx(s.ctx)
	s.acker = athena.NewACKer()
	s.batch = ctx.Properties().GetInt(BatchSizeProperty)
	echoType := ctx.Properties().GetString(TypeProperty)
	if s.buffer == nil {
		s.buffer = queue.New()
	}
	switch echoType {
	case "debug":
		s.echoFunc = s.logger.Debugf
	case "warn":
		s.echoFunc = s.logger.Warnf
	case "error":
		s.echoFunc = s.logger.Errorf
	case "info":
		s.echoFunc = s.logger.Infof
	default:
		s.logger.Warnf("unknown echo type %s, use info", echoType)
		s.echoFunc = s.logger.Infof
	}
	<-s.ctx.Done()
	return nil
}

func (s *sink) Close() error {
	return nil
}

func (s *sink) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{BatchSizeProperty, TypeProperty}
}

//New uses for test only
func New() athena.Sink {
	return &sink{}
}

func init() {
	component.RegisterNewSinkFunc("echo", New)
}
