package athena

import (
	"bytes"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"strconv"
)

//Properties is a subset of *viper.Viper
type Properties interface {
	Get(key string) interface{}
	Sub(key string) Properties
	IsSet(key string) bool
}

type Property interface {
	Name() string
	Description() string
	Type() string
	Required() bool
	Default() interface{}
}

type PropertyDef []Property

func (p PropertyDef) Render() string {
	buffer := &bytes.Buffer{}
	tWriter := tablewriter.NewWriter(buffer)
	tWriter.SetHeader([]string{"name", "description", "required", "type", "default"})
	tWriter.SetAutoFormatHeaders(false)
	tWriter.SetAutoWrapText(false)
	for _, p := range p {
		tWriter.Append([]string{
			p.Name(),
			p.Description(),
			strconv.FormatBool(p.Required()),
			p.Type(),
			fmt.Sprintf("%+v", p.Default()),
		})
	}
	tWriter.Render()
	return buffer.String()
}
