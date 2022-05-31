package spooldir

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/log"
	"athena/lib/properties"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/cast"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"
)

var (
	ScanProperty       = properties.NewRequiredProperty[string]("scan", "watch this file and combine")
	BackupProperty     = properties.NewProperty[string]("backup", "if backup is nil, remove file after combine", "")
	PatternProperty    = properties.NewProperty[string]("pattern", "regex pattern", ".*")
	ConcurrentProperty = properties.NewProperty[int]("concurrent", "combine number", 1)
)

type source struct {
	ctx         athena.Context
	logger      athena.Logger
	scanDir     string
	backupDir   string
	fileSuffix  string
	pattern     *regexp.Regexp
	combinePool *ants.PoolWithFunc

	emitNext athena.EmitNext
	state    sync.Map
	mutex    sync.Mutex
}

func (s *source) Snapshot() ([]byte, error) {
	var buffer bytes.Buffer
	s.mutex.Lock()
	defer s.mutex.Unlock()
	snapshotMap := map[Identify]int64{}
	s.state.Range(func(key, value any) bool {
		snapshotMap[key.(Identify)] = value.(int64)
		return true
	})
	decoder := gob.NewEncoder(&buffer)
	if err := decoder.Encode(&snapshotMap); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *source) Restore(snapshot []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	snapshotMap := map[Identify]int64{}
	decoder := gob.NewDecoder(bytes.NewReader(snapshot))
	if err := decoder.Decode(&snapshotMap); err != nil {
		return err
	}
	for key, value := range snapshotMap {
		s.state.Store(key, value)
	}
	return nil

}

func (s *source) Open(ctx athena.Context) (err error) {
	s.ctx = ctx
	s.logger = log.Ctx(s.ctx)
	s.scanDir = ctx.Properties().GetString(ScanProperty)
	s.backupDir = ctx.Properties().GetString(BackupProperty)

	s.pattern, err = regexp.Compile(ctx.Properties().GetString(PatternProperty))
	if err != nil {
		return err
	}

	s.combinePool, err = ants.NewPoolWithFunc(ctx.Properties().GetInt(ConcurrentProperty),
		func(arg interface{}) {
			s.combine(cast.ToString(arg))
		},
		ants.WithLogger(&log.TailLoggerWrapper{s.logger}),
		ants.WithPanicHandler(func(reason interface{}) {
			if reason != nil {
				s.logger.Errorw("combine panic.", "reason", reason)
			}
		}))
	if err != nil {
		return err
	}
	return nil
}

func (s *source) Close() error {
	s.combinePool.Release()
	return nil
}

func (s *source) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{ScanProperty, BackupProperty, PatternProperty, ConcurrentProperty}
}

func (s *source) Collect(emitNext athena.EmitNext) error {
	s.emitNext = emitNext
	if err := s.recoveryCombine(); err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err = watcher.Add(s.scanDir); err != nil {
		return err
	}
	for {
		select {
		case <-s.ctx.Done():
			return watcher.Close()
		case e := <-watcher.Events:
			if e.Op&fsnotify.Create == fsnotify.Create {
				s.logger.Infof("scan to new files:%s.", e.Name)
				if s.pattern.MatchString(e.Name) {
					s.submitCombine(e.Name)
				}
			}
		case err = <-watcher.Errors:
			s.logger.Warnw("watch file system failed.", "err", err)
		}
	}
}

func (s *source) submitCombine(filePath string) {
	err := s.combinePool.Invoke(filePath)
	if err != nil {
		s.logger.Errorw(fmt.Sprintf("submit %s combine task error, skin file.", filePath), "err", err)
	}
}

func (s *source) recoveryCombine() error {
	identifyMap := map[Identify]string{}
	err := filepath.Walk(s.scanDir, func(path string, info fs.FileInfo, err error) error {
		identifyMap[convertStatToIdentify(info.Sys().(*syscall.Stat_t))] = path
		return nil
	})
	if err != nil {
		return err
	}
	//recovery combine file
	s.state.Range(func(key, value any) bool {
		filePath := identifyMap[key.(Identify)]
		if filePath != "" && s.pattern.MatchString(filePath) {
			s.submitCombine(filePath)
		}
		s.state.Delete(key)
		return true
	})
	return nil
}

func (s *source) combine(filePath string) {
	fileId, err := convertPathToIdentify(filePath)
	if err != nil {
		s.logger.Errorw("can't convert to identify,skip file.", "path", filePath, "err", err)
		return
	}
	var offset int64 = 0
	if offsetI, ok := s.state.Load(fileId); !ok {
		s.logger.Debug("can't found file Identify offset.")
	} else {
		offset = offsetI.(int64)
	}
	tailFile, err := tail.TailFile(filePath, tail.Config{
		Location: &tail.SeekInfo{
			Offset: offset,
			Whence: io.SeekStart,
		},
		Logger: &log.TailLoggerWrapper{Logger: s.logger}})
	if err != nil {
		s.logger.Errorw("tail error, skip this file.", "path", filePath, "err", err)
		return
	}
	for {
		select {
		case line, ok := <-tailFile.Lines:
			if ok {
				s.emitNext(
					&athena.Event{
						Meta:    map[string]interface{}{"file": filePath, "time": line.Time},
						Message: line.Text,
						Time:    time.Now(),
					}, nil)
			} else {
				s.logger.Debugf("combine %s done, start afterCombine.", filePath)
				s.afterCombine(filePath, fileId)
				return
			}
		case <-s.ctx.Done():
			s.logger.Info("ctx done, stopping tail and save position to state.")
			if tell, err := tailFile.Tell(); err != nil {
				s.logger.Errorw("un tell file, state error.", "err", err)
			} else {
				s.state.Store(fileId, tell)
				for {
					line, ok := <-tailFile.Lines
					if !ok {
						break
					}
					s.emitNext(
						&athena.Event{
							Meta:    map[string]interface{}{"file": filePath, "time": line.Time},
							Message: line.Text,
							Time:    time.Now(),
						}, nil)
				}
			}
		}
	}
}

func (s *source) afterCombine(filePath string, fileId Identify) {
	if s.backupDir == "" {
		//remove file
		if err := os.Remove(filePath); err != nil {
			s.logger.Errorw("can't remove.", "path", filePath, "err", err)
			return
		}
	} else {
		//backup file
		backupPath := path.Join(s.backupDir, path.Base(filePath)+time.Now().Format(".20060102150405"))
		if err := os.Rename(filePath, backupPath); err != nil {
			s.logger.Errorw("can't rename", "path", filePath, "err", err)
			return
		}
	}
	s.state.Delete(fileId)
	s.logger.Debugf("after combine %s.", filePath)
}

func New() athena.Source {
	return &source{}
}

func init() {
	component.RegisterNewSourceFunc("spooldir", New)
}
