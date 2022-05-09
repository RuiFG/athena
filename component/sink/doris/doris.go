package doris

import (
	"athena"
	"athena/event"
	"athena/properties"
	"github.com/sirupsen/logrus"
	"sync"
)

var (
	FrontendsProperty = properties.NewProperty("frontends", "doris stream load frontends", []string{})
	UserProperty      = properties.NewProperty("user", "doris db user", nil)
	PasswordProperty  = properties.NewProperty("password", "doris db password", nil)
	BatchProperty     = properties.NewProperty("batch", "doris stream load batch", 10000000)
)

type sink struct {
	interval    int
	batchSize   int64
	buffer      event.Ptrs
	size        int64
	bufferMutex sync.Mutex
	doneChan    chan struct{}
	echoChan    chan struct{}
	ctx         athena.Context
	logger      *logrus.Entry
}

func (s *sink) Open(ctx athena.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s *sink) Close() error {
	//TODO implement me
	panic("implement me")
}

func (s *sink) PropertyDef() athena.PropertyDef {
	//TODO implement me
	panic("implement me")
}

func (s *sink) Emit(ptr event.Ptr) {

}
