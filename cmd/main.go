package main

import (
	_ "athena/external"
	_ "athena/lib"
	"fmt"
	"os"
)

func main() {

	if err := Command.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
