package constant

import (
	"athena/lib/properties"
)

var (
	//runtime property

	RuntimeModeProperty      = properties.NewProperty[string]("mode", "athena work mode, ack or snapshot.", "ack")
	RuntimeLogLevelProperty  = properties.NewRequiredProperty[string]("log-level", "log-level")
	RuntimeStatusDirProperty = properties.NewProperty[string]("status-dir", "status-dir", ".")

	//component property

	TypeProperty = properties.NewRequiredProperty[string]("type", "component type")

	SelectorProperty = properties.NewRequiredProperty[string]("select", "emit select")
)
