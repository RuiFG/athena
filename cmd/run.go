package main

import (
	_ "athena/component"
	"athena/engine"
	_c "context"
	"github.com/spf13/cobra"
	"path"
)

func init() {
	Command.AddCommand(&cobra.Command{
		Use:   "run",
		Short: "run one punch man",
		Long:  `config source operator sink, start one punch man`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				panic("config file can't be nil")
			}
			configFilePath := args[0]
			e := engine.New(_c.Background(), path.Base(configFilePath), path.Ext(configFilePath)[1:], path.Dir(configFilePath))
			e.Run()
		},
	})
}
