package athena

//Stateful is event trans mode
type Stateful interface {
	//Snapshot will snapshot  component state after close
	Snapshot() ([]byte, error)

	//Restore will restore component state after open
	Restore(snapshot []byte) error
}

type Committal interface {
	PreCommit() error
	Commit() error
}

type EmitConfigurator interface {
	Config(map[string]EmitNext) EmitNext
}
