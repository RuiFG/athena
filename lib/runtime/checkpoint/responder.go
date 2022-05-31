package checkpoint

type Responder interface {
	TriggerCheckpoint(checkpointId int64) error
	GetName() string
}

//type ResponderExecutor struct {
//	responder chan<- *Notify
//	task      StreamTask
//}
