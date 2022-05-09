package properties

import (
	"athena"
	"bytes"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var (
	ErrPropertyNoSet = fmt.Errorf("property is requied,but not set")
	ErrPropertyIsNil = fmt.Errorf("property and proerty default is nil")
)

func New(propertiesName string, propertiesType string, propertiesPath ...string) *viper.Viper {
	v := viper.New()
	v.SetConfigName(propertiesName)
	v.SetConfigType(propertiesType)
	for _, p := range propertiesPath {
		v.AddConfigPath(p)
	}
	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("read config error:%s", err.Error()))
	}
	return v
}

func InitPropertyDef(ctx athena.Context, def athena.PropertyDef) (string, error) {
	buffer := &bytes.Buffer{}
	tWriter := tablewriter.NewWriter(buffer)
	tWriter.SetHeader([]string{"name", "type", "value"})
	tWriter.SetAutoFormatHeaders(false)
	tWriter.SetAutoWrapText(false)
	properties := ctx.Properties()
	for _, p := range def {
		if p.Required() {
			if !properties.IsSet(p.Name()) {
				return "", errors.WithMessage(ErrPropertyNoSet, p.Name())
			}
		} else {
			if p.Default() == nil && !properties.IsSet(p.Name()) {
				return "", errors.WithMessage(ErrPropertyIsNil, p.Name())
			} else {
				properties.SetDefault(p.Name(), p.Default())
			}
		}
		tWriter.Append([]string{
			p.Name(),
			p.Type(),
			fmt.Sprintf("%+v", properties.Get(p.Name())),
		})
	}
	tWriter.Render()
	return buffer.String(), nil
}
