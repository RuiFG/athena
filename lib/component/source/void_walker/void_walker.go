package void_walker

import (
	"athena/athena"
	"athena/lib/component/source/void_walker/tail"
	"athena/lib/log"
	"athena/lib/properties"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	AccessLogProperty = properties.NewRequiredProperty[string]("access-log", "currentTail access log path")
	VWFolderProperty  = properties.NewRequiredProperty[string]("vm-folder", "access log vm folder")
)

const tailPositionSuffix string = "tail_position"

type TailingInfo struct {
	fileName string
	file     *os.File
	tail     *tail.Tail
}

func getTailPositionName(accessLogPath string) string {
	return fmt.Sprintf("%s.%s", filepath.Base(accessLogPath), tailPositionSuffix)
}

type source struct {
	ctx athena.Context

	logger athena.Logger

	lock      sync.Mutex
	accessLog string
	vwFolder  string

	fileWalker         *FileWalker
	currentFileWrapper *FileWrapper
	currentTail        *tail.Tail
	lastRowIdentify    *RowIdentify
}

func (s *source) Open(ctx athena.Context) error {

	s.ctx = ctx
	s.logger = log.Ctx(s.ctx)
	s.accessLog = ctx.Properties().GetString(AccessLogProperty)
	s.vwFolder = ctx.Properties().GetString(VWFolderProperty)

	s.lastRowIdentify = loadLastPositions(getTailPositionName(s.accessLog))
	if s.lastRowIdentify == nil {
		s.lastRowIdentify = &RowIdentify{
			&FileIdentify{0, 0}, 0,
		}
	}

	fileWalker, fresh, err := NewFileWalker(s.accessLog, s.lastRowIdentify.FileIdentify, s.vwFolder)
	if err != nil {
		return err
	}
	if fresh {
		s.lastRowIdentify.Position = 0
	}
	s.fileWalker = fileWalker
	return nil
}

func (s *source) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{AccessLogProperty, VWFolderProperty}
}

func (s *source) Close() error {
	s.currentTail.Stop()
	s.currentTail.Wait()
	restorePosition(getTailPositionName(s.accessLog), &RowIdentify{
		s.currentFileWrapper.FileIdentify,
		s.currentTail.LastPosition,
	})

	return nil
}

func (s *source) Collect(emitNext athena.EmitNext) error {
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}

		fileWrapper := s.fileWalker.GetNextFile()
		if fileWrapper.Path != s.accessLog {
			fileWrapper.Path = "fake " + fileWrapper.Path
		}
		s.currentFileWrapper = fileWrapper
		s.currentTail = tail.NewTail(fileWrapper.Path, fileWrapper.File, tail.Config{
			Location: &tail.SeekInfo{
				Offset: 0,
				Whence: io.SeekStart,
			},
			Logger: &log.TailLoggerWrapper{Logger: s.logger},
		})
		for line := range s.currentTail.Lines {
			emitNext(&athena.Event{
				Meta:    map[string]any{"fileWrapper": fileWrapper.Path},
				Message: line.Text,
				Time:    line.Time,
			}, nil)
		}
	}

}
