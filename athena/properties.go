package athena

import "time"

//Properties is a subset of *viper.Viper
type Properties interface {
	Global() Properties
	Sub(key string) Properties
	IsSet(key string) bool
	PrefixKeys(prefix string) []string

	GetStringSlice(property Property) []string
	GetString(property Property) string
	GetInt(property Property) int
	GetUint64(property Property) uint64
	GetDuration(property Property) time.Duration
}

type Property interface {
	Name() string
	Description() string
	Type() string
	Required() bool
	Default() interface{}
}

type PropertiesDef []Property
