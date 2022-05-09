package echo

import (
	"athena"
	"athena/event"
	"athena/pkg/queue"
	"athena/properties"
	"bytes"
	"encoding/gob"
	"sync"
)

var (
	BatchSizeProperty = properties.NewProperty[int]("batch", "echo sink echo batch size", 100)
	TypeProperty      = properties.NewProperty[string]("echo", "echo type, like info debug", "info")
)

type buffer struct {
	Size  int64
	Slice []event.Ptr
}

type sink struct {
	batch     int
	buffer    *queue.Queue
	bufferMux sync.Mutex
	ctx       athena.Context
	echoFunc  func(format string, args ...interface{})
}

func (s *sink) Snapshot() ([]byte, error) {
	var (
		b        bytes.Buffer
		ptrSlice []event.Ptr
	)
	decoder := gob.NewEncoder(&b)
	for i := 0; i < s.buffer.Length(); i++ {
		ptrSlice = append(ptrSlice, s.buffer.Remove().(event.Ptr))
	}
	if err := decoder.Encode(&ptrSlice); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (s *sink) Restore(snapshot []byte) error {
	var ptrSlice []event.Ptr
	decoder := gob.NewDecoder(bytes.NewReader(snapshot))
	if err := decoder.Decode(&ptrSlice); err != nil {
		return err
	}
	s.buffer = queue.New()
	for _, ptr := range ptrSlice {
		s.buffer.Add(ptr)
	}
	return nil
}

func (s *sink) Emit(ptr event.Ptr) {
	s.bufferMux.Lock()
	defer s.bufferMux.Unlock()
	s.buffer.Add(ptr)
	if s.buffer.Length() >= s.batch {
		for i := 0; i < s.batch; i++ {
			s.echoFunc("%+v", s.buffer.Remove())
		}
	}

}

func (s *sink) Delete(name string) {
	//nothing to do
}

func (s *sink) Open(ctx athena.Context) error {
	s.ctx = ctx
	s.batch = ctx.Properties().GetInt(BatchSizeProperty.Name())
	echoType := ctx.Properties().GetString(TypeProperty.Name())
	if s.buffer == nil {
		s.buffer = queue.New()
	}
	switch echoType {
	case "debug":
		s.echoFunc = s.ctx.Logger().Debugf
	case "warn":
		s.echoFunc = s.ctx.Logger().Warnf
	case "error":
		s.echoFunc = s.ctx.Logger().Errorf
	case "info":
		s.echoFunc = s.ctx.Logger().Infof
	default:
		s.ctx.Logger().Warnf("unknown echo type %s, use info", echoType)
		s.echoFunc = s.ctx.Logger().Infof
	}
	<-s.ctx.Done()
	return nil
}

func (s *sink) Close() error {
	return nil
}

func (s *sink) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{BatchSizeProperty, TypeProperty}
}

//New uses for test only
func New() athena.Sink {
	return &sink{}
}
