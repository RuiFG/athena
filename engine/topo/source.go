package topo

import (
	"athena"
)

type SourceTaskOption func(task *SourceTask)

type SourceTask struct {
	*ComponentTask
	ctx athena.Context

	source            athena.Source
	emitNextGenerator EmitNextGenerator
}

func (s *SourceTask) Starter() error {
	if err := s.source.Open(s.ctx); err != nil {
		return err
	}
	return s.source.Collect(s.emitNextGenerator())
}

func (s *SourceTask) Stopper() error {
	return s.source.Close()
}

func NewSourceBox(ctx athena.Context, name string, source athena.Source, emitNextGenerator EmitNextGenerator, options ...SourceTaskOption) *SourceTask {
	sourceBox := &SourceTask{source: source,
		ComponentTask: &ComponentTask{
			name:      name,
			component: source,
			ctx:       ctx,
		},
		ctx:               ctx,
		emitNextGenerator: emitNextGenerator,
	}
	sourceBox.starter = sourceBox.Starter
	sourceBox.stopper = sourceBox.Stopper
	for _, option := range options {
		option(sourceBox)
	}
	return sourceBox
}

var _ Task = &SourceTask{}
