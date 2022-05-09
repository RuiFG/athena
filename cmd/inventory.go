package main

import (
	"athena"
	"athena/registry"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	Command.AddCommand(&cobra.Command{
		Use:   "inventory",
		Short: "list vesta source operator sink inventory.",
		Long:  `list vesta source operator sink inventory.`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				panic("inventory type can't be nil.")
			}
			var defs map[string]athena.PropertyDef

			switch args[0] {
			case "source":
				defs = registry.ListSourceDef()
			case "operator":
				defs = registry.ListOperatorDef()
			case "sink":
				defs = registry.ListSinkDef()
			default:
				panic("unknown inventory type.")
			}

			for name, def := range defs {
				fmt.Printf("%s %s:\n%s\n", name, args[0], def.Render())
			}
		}})
}
