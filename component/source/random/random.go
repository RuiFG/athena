package random

import (
	"athena"
	"athena/event"
	"athena/properties"
	"math/rand"
	"time"
)

var (
	IntervalProperty = properties.NewProperty[int]("interval", "random source generate record interval", 100)
)

type source struct {
	ctx      athena.Context
	interval int
}

func (s *source) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{IntervalProperty}
}

func (s *source) Collect(emitNext athena.EmitNext) error {
	//open source
	ticker := time.NewTicker(time.Duration(s.interval) * time.Millisecond)
	for true {
		select {
		case <-s.ctx.Done():
			//source close
			return nil
		case <-ticker.C:
			emitNext(event.MustNewPtr(map[string]interface{}{}, map[string]interface{}{"random": rand.Int63()}))
		}
	}
	return nil
}

func (s *source) Open(ctx athena.Context) error {
	s.ctx = ctx
	s.interval = ctx.Properties().GetInt(IntervalProperty.Name())

	return nil
}

func (s *source) Close() error {
	return nil
}

//New uses for test only
func New() athena.Source {
	return &source{}
}
