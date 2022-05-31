package properties

import (
	"athena/athena"
	"bytes"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"strconv"
	"time"
)

var (
	ErrPropertyNoSet = fmt.Errorf("property is requied,but not set")
	ErrPropertyIsNil = fmt.Errorf("property and proerty default is nil")
)

type properties struct {
	*viper.Viper
	runtime *viper.Viper
}

func (p *properties) Sub(key string) athena.Properties {
	return &properties{Viper: p.Viper.Sub(key), runtime: p.runtime}
}

func (p *properties) PrefixKeys(prefix string) []string {
	all := p.Viper.GetStringMap(prefix)
	keys := make([]string, 0)
	for key := range all {
		keys = append(keys, key)
	}
	return keys
}

func (p *properties) Global() athena.Properties {
	return &properties{Viper: p.runtime, runtime: p.runtime}
}

func (p *properties) GetStringSlice(property athena.Property) []string {
	return p.Viper.GetStringSlice(property.Name())

}
func (p *properties) GetString(property athena.Property) string {
	return p.Viper.GetString(property.Name())
}

func (p *properties) GetInt(property athena.Property) int {
	return p.Viper.GetInt(property.Name())
}

func (p *properties) GetUint64(property athena.Property) uint64 {
	return p.Viper.GetUint64(property.Name())
}

func (p *properties) GetDuration(property athena.Property) time.Duration {
	return p.Viper.GetDuration(property.Name())
}

func InitAndRender(p athena.Properties, def athena.PropertiesDef) (string, error) {
	switch _p := p.(type) {
	case *properties:
		buffer := &bytes.Buffer{}
		tWriter := tablewriter.NewWriter(buffer)
		tWriter.SetHeader([]string{"name", "type", "value"})
		tWriter.SetAutoFormatHeaders(false)
		tWriter.SetAutoWrapText(false)

		for _, _property := range def {
			if _property.Required() {
				if !_p.Viper.IsSet(_property.Name()) {
					return "", errors.WithMessage(ErrPropertyNoSet, _property.Name())
				}
			} else {
				if _property.Default() == nil && !_p.Viper.IsSet(_property.Name()) {
					return "", errors.WithMessage(ErrPropertyIsNil, _property.Name())
				} else {
					_p.Viper.SetDefault(_property.Name(), _property.Default())
				}
			}
			tWriter.Append([]string{
				_property.Name(),
				_property.Type(),
				fmt.Sprintf("%+v", _p.Viper.Get(_property.Name())),
			})
		}
		tWriter.Render()
		return buffer.String(), nil
	default:
		return "", nil
	}
}

func RenderDef(p athena.PropertiesDef) string {
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

func New(propertiesName string, propertiesType string, propertiesPath ...string) athena.Properties {
	v := viper.New()
	v.SetConfigName(propertiesName)
	v.SetConfigType(propertiesType)
	for _, p := range propertiesPath {
		v.AddConfigPath(p)
	}
	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("read config error:%s", err.Error()))
	}
	return &properties{Viper: v, runtime: v.Sub("global")}
}
