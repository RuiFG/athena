package main

import (
	"athena/lib/runtime"
	_c "context"
	"github.com/spf13/cobra"
	"path"
)

func init() {
	Command.AddCommand(&cobra.Command{
		Use:   "run",
		Short: "run [config file]",
		Long:  `config source operator sink, start vesta`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				panic("config file can't be nil")
			}
			configFilePath := args[0]
			e := runtime.New(_c.Background(), path.Base(configFilePath), path.Ext(configFilePath)[1:], path.Dir(configFilePath))
			e.Run()
		},
	})
}
