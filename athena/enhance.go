package athena

//Stateful is event trans mode, for snapshot mode
type Stateful interface {
	//Snapshot will snapshot  component state after close
	Snapshot() ([]byte, error)

	//Restore will restore component state after open
	Restore(snapshot []byte) error
}
