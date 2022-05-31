package log

import (
	"athena/athena"
)

type TailLoggerWrapper struct {
	athena.Logger
}

func (l *TailLoggerWrapper) Fatalln(v ...interface{}) {
	l.Logger.Fatal(v...)
}

func (l *TailLoggerWrapper) Panic(v ...interface{}) {
	l.Logger.Error(v...)
	panic(v)
}

func (l *TailLoggerWrapper) Panicf(format string, v ...interface{}) {
	l.Logger.Errorf(format, v...)
	panic(v)
}

func (l *TailLoggerWrapper) Panicln(v ...interface{}) {
	l.Logger.Error(v...)
	panic(v)
}

func (l *TailLoggerWrapper) Print(v ...interface{}) {
	l.Logger.Info(v...)
}

func (l *TailLoggerWrapper) Println(v ...interface{}) {
	l.Logger.Info(v...)
}

func (l *TailLoggerWrapper) Printf(format string, args ...interface{}) {
	l.Logger.Infof(format, args...)
}
