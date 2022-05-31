package lib

import (
	//source
	_ "athena/lib/component/source/kafka"
	_ "athena/lib/component/source/mock"
	_ "athena/lib/component/source/spooldir"

	//operator
	_ "athena/lib/component/operator/sample"
	_ "athena/lib/component/operator/tengo"
	//sink
	_ "athena/lib/component/sink/echo"

	//emit
	_ "athena/lib/emit/replicating"
)
