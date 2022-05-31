package main

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/properties"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	Command.AddCommand(&cobra.Command{
		Use:   "component",
		Short: "list athena source operator sink.",
		Long:  `list athena source operator sink.`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				panic("inventory type can't be nil.")
			}
			var defs map[string]athena.PropertiesDef

			switch args[0] {
			case "source":
				defs = component.ListSourceDef()
			case "operator":
				defs = component.ListOperatorDef()
			case "sink":
				defs = component.ListSinkDef()
			default:
				panic("unknown component type.")
			}

			for name, def := range defs {
				fmt.Printf("%s %s:\n%s\n", name, args[0], properties.RenderDef(def))
			}
		}})
}
