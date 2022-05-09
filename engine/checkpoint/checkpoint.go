package checkpoint

import (
	"athena/event"
)

const checkpointId = "connector$checkpointId"

func IsCheckpoint(e event.Ptr) bool {
	return e.Meta[checkpointId] != nil && e.Message == nil
}
