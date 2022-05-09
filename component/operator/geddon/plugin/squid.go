package plugin

import (
	"athena/component/operator/geddon/log_format/nginx"
	"fmt"
	"runtime/debug"
)

func safeAddSquid(f func(ptr *nginx.LogExt), ptr *nginx.LogExt) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err.(error).Error())
			debug.PrintStack()
		}
	}()

	f(ptr)
}
