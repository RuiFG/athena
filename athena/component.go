package athena

const (
	ACK      = "ack"
	Snapshot = "snapshot"
)

//Emit describe operator sink Emit Func
type Emit func(event *Event)

type EmitGenerator func(upstreamCtx Context) Emit

//EmitNext send event to next component,
//handler is called at ack time in ACK mode or send time in Snapshot mode
type EmitNext func(event *Event, handler ACKHandler)

//EmitNextGenerator  generate EmitNext from EmitGenerator
type EmitNextGenerator func(ctx Context, allEmitGenerator map[Context]EmitGenerator, topology map[Context][]Context) EmitNext

//Component is core
type Component interface {
	//Open initialize the component
	Open(ctx Context) error
	//Close cleaning up after the context done.
	Close() error
	//PropertiesDef return Component properties defend
	PropertiesDef() PropertiesDef
}

type Source interface {
	Component
	//Collect should block caller,and wait for ctx done or source done.
	Collect(emitNext EmitNext) error
}

type Operator interface {
	Component
	//Collect should block caller,and wait for ctx done or operator done.
	Collect(emitNext EmitNext) error
	//GenerateEmit is a method to receive events
	GenerateEmit(upstreamCtx Context) Emit
}

type Sink interface {
	Component
	//GenerateEmit is a method to receive events
	GenerateEmit(upstreamCtx Context) Emit
}

type NewSourceFunc func() Source
type NewSinkFunc func() Sink
type NewOperatorFunc func() Operator

type NewEmitNextGeneratorFunc func() EmitNextGenerator
