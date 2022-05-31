package checkpoint

import (
	"athena/athena"
)

const checkpointId = "connector$checkpointId"

func IsCheckpoint(e *athena.Event) bool {
	return e.Meta[checkpointId] != nil && e.Message == nil
}
