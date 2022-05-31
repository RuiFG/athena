package task

import (
	"athena/athena"
)

type SinkTask struct {
	athena.Sink
	Ctx athena.Context
}

func (s *SinkTask) Run() error {
	if err := s.Open(s.Ctx); err != nil {
		return err
	}
	//Sink does not block, so wait
	<-s.Ctx.Done()
	return s.Close()
}
