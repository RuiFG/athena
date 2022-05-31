package task

import (
	"athena/athena"
)

type SourceTask struct {
	athena.Source
	Ctx      athena.Context
	EmitNext athena.EmitNext
	Name     string
}

func (s *SourceTask) Run() error {
	if err := s.Open(s.Ctx); err != nil {
		return err
	}
	if err := s.Collect(s.EmitNext); err != nil {
		return err
	}
	return s.Close()
}
