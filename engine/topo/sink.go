package topo

import (
	"athena"
	"athena/event"
	"gopkg.in/tomb.v2"
)

type SinkTaskOption func(task *SinkTask)

type SinkTask struct {
	*ComponentTask
	ctx  athena.Context
	life tomb.Tomb
	sink athena.Sink
}

func (s *SinkTask) Emit(ptr event.Ptr) {
	s.sink.Emit(ptr)
}

func (s *SinkTask) Starter() error {
	if err := s.sink.Open(s.ctx); err != nil {
		return err
	}
	//sink does not block, so wait
	<-s.ComponentTask.ctx.Done()
	return nil
}

func (s *SinkTask) Stopper() error {
	return s.sink.Close()
}

func NewSinkBox(ctx athena.Context, name string, sink athena.Sink, options ...SinkTaskOption) *SinkTask {
	var sinkBoxWrapper = &SinkTask{
		sink: sink,
		ctx:  ctx,
		ComponentTask: &ComponentTask{
			name:      name,
			component: sink,
			ctx:       ctx,
		},
	}
	sinkBoxWrapper.starter = sinkBoxWrapper.Starter
	sinkBoxWrapper.stopper = sinkBoxWrapper.Stopper
	for _, option := range options {
		option(sinkBoxWrapper)
	}
	return sinkBoxWrapper
}

var _ NonRootTask = &SinkTask{}
