package geddon

import (
	"athena"
	"athena/event"
	"athena/properties"
	"encoding/gob"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
)

var (
	OutputProperty = properties.NewProperty[string]("output", "geddon output path of statis files", "/opt/geddon/statis_out")
)

type sink struct {
	output string
	ctx    athena.Context
	writer func(ptr event.Ptr)
}

func (s *sink) Open(ctx athena.Context) error {
	s.ctx = ctx
	s.output = ctx.Properties().GetString(OutputProperty.Name())
	return nil
}

func (s *sink) Close() error {
	return nil
}

func (s *sink) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{OutputProperty}
}

func (s *sink) Emit(ptr event.Ptr) {
	switch ptr.Meta["type"] {
	case "json":
		s.writeJson(ptr)
	case "gob":
		s.writeGOB(ptr)
	case "proto":
		s.writeProto(ptr)
	default:
		s.ctx.Logger().Warnf("can't fount ptr meta type, discard event.")
	}
}

func (s *sink) writeGOB(ptr event.Ptr) {

	if v := reflect.ValueOf(ptr.Message); v.Kind() == reflect.Slice && v.Len() == 0 {
		return
	}

	s.ctx.Logger().Infof("writing: %s.", ptr.Meta["id"])

	fout, err := os.Create(path.Join(s.output, ptr.Meta["id"].(string)))
	defer fout.Close()
	if err != nil {
		s.ctx.Logger().WithError(err).Error("write to file error.")
		return
	}
	err = gob.NewEncoder(fout).Encode(ptr.Message)
	if err != nil {
		s.ctx.Logger().WithError(err).Error("write to file error")
		return
	}
}

func (s *sink) writeJson(ptr event.Ptr) {
	filename := path.Join(s.output, ptr.Meta["id"].(string))
	fn, err := filepath.Abs(filename)
	if err != nil {
		logrus.Errorln("output file error")
		return
	}

	logrus.Infoln("writing: ", fn)

	fout, err := os.Create(filename)
	defer fout.Close()
	if err != nil {
		logrus.Errorln("write to file error", err)
		return
	}
	bs, _ := json.MarshalIndent(ptr.Message, "", "	")
	fout.Write(bs)
}

func (s *sink) writeProto(ptr event.Ptr) {
	filename := path.Join(s.output, ptr.Meta["id"].(string))
	s.ctx.Logger().Infoln("writing: ", filename)
	protoMessage, ok := ptr.Message.(proto.Message)
	if !ok {
		s.ctx.Logger().Warnf("ptr message is not proto message, discord ptr.")
		return
	}
	buffer, err := proto.Marshal(protoMessage)
	if err != nil {
		s.ctx.Logger().WithError(err).Errorln("write to file error/or compact protocol buffer.")
		return
	}

	if err = ioutil.WriteFile(filename, buffer, 0644); err != nil {
		s.ctx.Logger().WithError(err).Errorln("write to file error/or compact protocol buffer.")
		return
	}
}

func New() athena.Sink {
	return &sink{}
}
