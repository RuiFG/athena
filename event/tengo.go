package event

import (
	"github.com/d5/tengo/v2"
	"github.com/pkg/errors"
	"time"
)

type TengoMeta struct {
	tengo.ObjectImpl
	Value map[string]any
}

func (m *TengoMeta) TypeName() string {
	return "event-meta"
}

func (m *TengoMeta) String() string {
	//TODO implement me
	return "<event-meta>"
}

func (m *TengoMeta) IsFalsy() bool {
	return len(m.Value) == 0
}

func (m *TengoMeta) Equals(another tengo.Object) bool {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMeta) Copy() tengo.Object {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMeta) IndexGet(index tengo.Object) (res tengo.Object, err error) {
	strIdx, ok := tengo.ToString(index)
	if !ok {
		err = tengo.ErrInvalidIndexType
		return
	}
	resA, ok := m.Value[strIdx]
	if !ok {
		res = tengo.UndefinedValue
	}
	switch v := resA.(type) {
	case int64:
		res = &tengo.Int{Value: v}
	case bool:
		if v {
			res = tengo.TrueValue
		} else {
			res = tengo.FalseValue
		}
	case string:
		res = &tengo.String{Value: v}
	case float64:
		res = &tengo.Float{Value: v}
	case rune:
		res = &tengo.Char{Value: v}
	case []byte:
		res = &tengo.Bytes{Value: v}
	case time.Time:
		res = &tengo.Time{Value: v}
	default:
		err = errors.New("event meta type should be int64,bool,string,float64,rune,[]byte,time.Time.")
	}
	return
}

func (m *TengoMeta) IndexSet(index, value tengo.Object) (err error) {
	strIdx, ok := tengo.ToString(index)
	if !ok {
		err = tengo.ErrInvalidIndexType
		return
	}
	toInterface := tengo.ToInterface(value)
	if toInterface == nil {
		m.Value[strIdx] = _empty{}
		return
	}
	switch v := tengo.ToInterface(value).(type) {
	case int64, bool, string, float64, rune, []byte, time.Time, _empty:
		m.Value[strIdx] = v
	default:
		err = ErrValidated
	}
	return
}

func (m *TengoMeta) Iterate() tengo.Iterator {
	//TODO implement me
	return nil
}

func (m *TengoMeta) CanIterate() bool {
	//TODO implement me
	return true
}

type TengoMessage struct {
	tengo.ObjectImpl
	Value any
}

func (m *TengoMessage) TypeName() string {
	return "event-message"
}

func (m *TengoMessage) String() string {
	//TODO implement me
	return "<event-message>"
}

func (m *TengoMessage) IsFalsy() bool {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMessage) Equals(another tengo.Object) bool {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMessage) Copy() tengo.Object {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMessage) IndexGet(index tengo.Object) (value tengo.Object, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMessage) IndexSet(index, value tengo.Object) error {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMessage) Iterate() tengo.Iterator {
	//TODO implement me
	panic("implement me")
}

func (m *TengoMessage) CanIterate() bool {
	//TODO implement me
	panic("implement me")
}

type TengoStruct struct {
	tengo.ObjectImpl
	Meta    *TengoMeta
	Message *TengoMessage
	Time    *tengo.Time
}

// TypeName returns the name of the type.
func (s *TengoStruct) TypeName() string {
	return "event"
}

func (s *TengoStruct) String() string {
	return "<event>"
}

func (s *TengoStruct) IsFalsy() bool {
	return s.Message.Value == EmptyS && len(s.Meta.Value) == 0 && s.Time.Value == EmptyT
}

func (s *TengoStruct) IndexGet(o tengo.Object) (tengo.Object, error) {
	strIdx, ok := tengo.ToString(o)
	if !ok {
		return nil, tengo.ErrInvalidIndexType
	}
	switch strIdx {
	case "Meta":
		return s.Meta, nil
	case "Message":
		return s.Message, nil
	case "Time":
		return s.Time, nil
	default:
		return tengo.UndefinedValue, nil
	}
}

func (s *TengoStruct) IndexSet(index, value tengo.Object) error {
	strIdx, ok := tengo.ToString(index)
	if !ok {
		return tengo.ErrInvalidIndexType
	}

	switch strIdx {
	case "Meta":
		toInterface := tengo.ToInterface(value)
		switch v := toInterface.(type) {
		case map[string]any:
			s.Meta = &TengoMeta{Value: v}
		default:
			return errors.New("meta type should be map[string]any.")
		}
	case "Message":
		toInterface := tengo.ToInterface(value)
		if toInterface == nil {
			s.Message = &TengoMessage{Value: EmptyS}
		}
		switch v := toInterface.(type) {
		case int64, bool, string, float64, rune, []byte, time.Time:
			s.Message = &TengoMessage{Value: v}
		default:
			return errors.Errorf("message un support %s", value.TypeName())
		}
	case "Time":
		if v, ok := value.(*tengo.Time); !ok {
			return errors.New("time type should be time.Time")
		} else {
			s.Time = v
		}
	default:
		return errors.New("unknown key")
	}
	return nil
}

func ToTengoPtr(ptr Ptr) tengo.Object {
	if ptr == nil {
		return tengo.UndefinedValue
	}
	return &TengoStruct{
		Meta:    &TengoMeta{Value: ptr.Meta},
		Message: &TengoMessage{Value: ptr.Message},
		Time:    &tengo.Time{Value: ptr.Time},
	}
}
