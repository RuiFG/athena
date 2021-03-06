package log

import (
	"fmt"
	"go.uber.org/zap/zapcore"
	"os"
)

type Level zapcore.Level

const (
	DebugLevel = Level(zapcore.DebugLevel)
	InfoLevel  = Level(zapcore.InfoLevel)
	WarnLevel  = Level(zapcore.WarnLevel)
	ErrorLevel = Level(zapcore.ErrorLevel)
	FatalLevel = Level(zapcore.FatalLevel)
	PanicLevel = Level(zapcore.PanicLevel)
)

type OutputEncoder func(cfg zapcore.EncoderConfig) zapcore.Encoder

var (
	JsonOutputEncoder    OutputEncoder = zapcore.NewJSONEncoder
	ConsoleOutputEncoder OutputEncoder = zapcore.NewConsoleEncoder
)

type CallerEncoder zapcore.CallerEncoder

var (
	FullCallerEncoder        CallerEncoder = zapcore.FullCallerEncoder
	FullRoutineCallerEncoder CallerEncoder = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(fmt.Sprintf("%d#%d %s", os.Getegid(), id(), caller.String()))
	}
	ShortCallerEncoder        CallerEncoder = zapcore.ShortCallerEncoder
	ShortRoutineCallerEncoder CallerEncoder = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(fmt.Sprintf("%d#%d %s", os.Getegid(), id(), caller.TrimmedPath()))
	}
)

type LevelEncoder zapcore.LevelEncoder

var (
	LowercaseLevelEncoder      LevelEncoder = zapcore.LowercaseLevelEncoder
	LowercaseColorLevelEncoder LevelEncoder = zapcore.LowercaseColorLevelEncoder
	CapitalLevelEncoder        LevelEncoder = zapcore.CapitalLevelEncoder
	CapitalColorLevelEncoder   LevelEncoder = zapcore.CapitalColorLevelEncoder
	BracketLevelEncoder        LevelEncoder = func(level zapcore.Level, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString("[" + level.String() + "]")
	}
)
