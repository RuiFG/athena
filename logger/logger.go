package logger

import (
	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
)

func New(lvl string) *logrus.Logger {
	l := logrus.New()
	formatter := &nested.Formatter{
		TimestampFormat: "2006/01/02 15:04:05",
		HideKeys:        true,
		FieldsOrder:     []string{"component"},
	}
	l.Formatter = formatter
	if level, err := logrus.ParseLevel(lvl); err != nil {
		panic(err)
	} else {
		l.Level = level
	}

	return l
}
