package doris

import (
	"athena/athena"
	"athena/lib/properties"
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
	buffer      []athena.Event
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

func (s *sink) PropertiesDef() athena.PropertiesDef {
	//TODO implement me
	panic("implement me")
}

func (s *sink) Emit(event *athena.Event) {

}
