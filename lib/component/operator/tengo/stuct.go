package tengo

import (
	"athena/athena"
	"fmt"
	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"time"
)

var (
	emptyTime  = time.Time{}
	emptyEvent = &_struct{
		Meta:    nil,
		Message: nil,
		Time:    &tengo.Time{Value: emptyTime},
	}
)

type _struct struct {
	tengo.ObjectImpl
	Meta    *tengo.Map
	Message tengo.Object
	Time    *tengo.Time
}

// TypeName returns the name of the type.
func (s *_struct) TypeName() string {
	return "event"
}

func (s *_struct) String() string {
	return "<event>"
}

func (s *_struct) IsFalsy() bool {
	return s.Message.IsFalsy() && s.Meta.IsFalsy() && s.Time.IsFalsy()
}

func (s *_struct) IndexGet(o tengo.Object) (tengo.Object, error) {
	strIdx, ok := tengo.ToString(o)
	if !ok {
		return nil, tengo.ErrInvalidIndexType
	}
	switch strIdx {
	case "meta":
		return s.Meta, nil
	case "message":
		return s.Message, nil
	case "time":
		return s.Time, nil
	default:
		return tengo.UndefinedValue, fmt.Errorf("unknown key %s", strIdx)
	}
}

func (s *_struct) IndexSet(index, value tengo.Object) error {
	strIdx, ok := tengo.ToString(index)
	if !ok {
		return tengo.ErrInvalidIndexType
	}

	switch strIdx {
	case "meta":
		if v, ok := value.(*tengo.Map); !ok {
			return fmt.Errorf("meta only support map, but received is %s", value.TypeName())
		} else {
			s.Meta = v
		}
	case "message":
		s.Message = value
	case "time":
		if v, ok := value.(*tengo.Time); !ok {
			return fmt.Errorf("time only support time.Time, but received is %s", value.TypeName())
		} else {
			s.Time = v
		}
	default:
		return fmt.Errorf("unknown key %s", strIdx)
	}
	return nil
}

func toTengoEvent(event *athena.Event) (tengo.Object, error) {
	tengoMessage, err := tengo.FromInterface(event.Message)
	if err != nil {
		return nil, errors.WithMessage(err, "message can't convert to tengo type.")
	}
	tengoMetaValue := make(map[string]tengo.Object)
	for key, value := range event.Meta {
		object, err := tengo.FromInterface(value)
		if err != nil {
			return nil, errors.WithMessagef(err, "meta %s key can't convert to tengo type.", key)
		}
		tengoMetaValue[key] = object
	}
	tengoMeta := &tengo.Map{Value: tengoMetaValue}
	tengoTime := &tengo.Time{Value: event.Time}
	return &_struct{
		Meta:    tengoMeta,
		Message: tengoMessage,
		Time:    tengoTime,
	}, nil
}
