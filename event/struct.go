package event

import (
	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"time"
)

//serialize

type Serializer func(event Ptr) ([]byte, error)
type Deserializer func([]byte) (Ptr, error)

//Ptr is not thread safety
type Ptr *_struct

type _empty struct{}

type _struct struct {
	Meta    map[string]any
	Message any
	Time    time.Time
}

func Copy(s Ptr) Ptr {
	return &_struct{Time: s.Time, Meta: deepCopyMeta(s.Meta), Message: deepCopyMessage(s.Message)}
}

func deepCopyMeta(meta map[string]any) map[string]any {
	newM := map[string]any{}
	//The meta has only one level of structure
	for key, value := range meta {
		newM[key] = value
	}
	return newM
}

func deepCopyMessage(message any) any {
	switch m := message.(type) {
	case int64, bool, string, float64, rune, []byte, time.Time, _empty:
		return m
	case map[string]any:
		newM := map[string]any{}
		for key, value := range m {
			newM[key] = deepCopyMessage(value)
		}
		return newM
	default:
		return m
	}
}

func validateMeta(meta map[string]any) bool {
	for _, value := range meta {
		switch value.(type) {
		case int64, bool, string, float64, rune, []byte, time.Time, _empty:
		default:
			return false
		}
	}
	return true
}

func validateMessage(message any) bool {
	switch m := message.(type) {
	case int64, bool, string, float64, rune, []byte, time.Time, _empty:
		return true
	case map[string]any:
		for _, value := range m {
			if !validateMessage(value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

//EmptyPtr is empty event
var (
	EmptyM   = map[string]any{}
	EmptyS   = _empty{}
	EmptyT   = time.Unix(0, 0)
	EmptyPtr = &_struct{Meta: EmptyM, Message: EmptyS}

	EmptyTengoPtr = &TengoStruct{
		Meta:    &TengoMeta{Value: EmptyM},
		Message: &TengoMessage{Value: EmptyS},
		Time:    &tengo.Time{Value: EmptyT},
	}

	ErrValidated = errors.New("value type should be int64, bool, string, float64, rune, []byte, time.Time, _empty")
)

func MustNewPtr(meta map[string]any, message any) Ptr {
	if ptr, err := NewPtr(meta, message); err != nil {
		panic(err)
	} else {
		return ptr
	}
}

func MustNewWithTime(meta map[string]any, message any, _time time.Time) Ptr {
	if ptr, err := NewWithTime(meta, message, _time); err != nil {
		panic(err)
	} else {
		return ptr
	}
}

func NewPtr(meta map[string]any, message any) (Ptr, error) {
	return NewWithTime(meta, message, time.Now())
}

func NewWithTime(meta map[string]any, message any, _time time.Time) (Ptr, error) {
	if meta == nil {
		meta = EmptyM
	}
	if message == nil {
		message = EmptyS
	}
	if !validateMeta(meta) {
		return EmptyPtr, errors.WithMessage(ErrValidated, "meta failed.")
	}
	if !validateMessage(message) {
		return EmptyPtr, errors.WithMessage(ErrValidated, "message failed.")
	}
	return &_struct{Meta: meta, Message: message, Time: _time}, nil
}
