// Copyright (c) 2016 Eycia Zhou. All rights reserved.
// Copyright (c) 2015 HPE Software Inc. All rights reserved.
// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package tail

import (
	"athena/lib/component/source/void_walker/tail/watch"
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

var (
	ErrStop = fmt.Errorf("tail should now stop")
)

type Line struct {
	Text string
	Time time.Time
	Err  error // Error from tail
}

// NewLine returns a Line with present time.
func NewLine(text string) *Line {
	return &Line{text, time.Now(), nil}
}

// SeekInfo represents arguments to `os.Seek`
type SeekInfo struct {
	Offset int64
	Whence int // os.SEEK_*
}

type logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Config is used to specify how a file must be tailed.
type Config struct {
	// File-specifc
	Location *SeekInfo // Seek to this location before tailing
	//Poll        bool      // Poll for file changes instead of using inotify

	// Logger, when nil, is set to tail.DefaultLogger
	// To disable logging: set field to tail.DiscardingLogger
	Logger logger
}

type Tail struct {
	Filename string
	Lines    chan *Line
	Config

	file   *os.File
	reader *bufio.Reader

	watcher   *watch.PollingFileWatcher
	tomb.Tomb // provides: Done, Kill, Dying

	LastPosition int64

	lk sync.Mutex
}

var (
	// DefaultLogger is used when Config.Logger == nil
	DefaultLogger = log.New(os.Stderr, "", log.LstdFlags)
	// DiscardingLogger can be used to disable logging output
	DiscardingLogger = log.New(ioutil.Discard, "", 0)
)

func NewTail(filename string, file *os.File, config Config) *Tail {
	t := &Tail{
		Filename: filename,
		Lines:    make(chan *Line),
		Config:   config,
	}

	// when Logger was not specified in config, use default logger
	if t.Logger == nil {
		t.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	t.watcher = watch.NewPollingFileWatcher(filename, file)

	if file == nil {
		panic("file can't be nil")
	}

	t.file = file

	t.Go(t.tailFileSync)

	return t
}

// Return the file's current position, like stdio's ftell().
// But this value is not very accurate.
// it may readed one line in the chan(tail.Lines),
// so it may lost one line.
func (tail *Tail) tell() (offset int64, err error) {
	if tail.file == nil {
		return
	}
	offset, err = tail.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return
	}

	tail.lk.Lock()
	defer tail.lk.Unlock()
	if tail.reader == nil {
		return
	}

	offset -= int64(tail.reader.Buffered())
	return
}

// Stop stops the tailing activity.
func (tail *Tail) Stop() error {
	tail.Kill(nil)
	return tail.Wait()
}

var errStopAtEOF = errors.New("tail: stop at eof")

func (tail *Tail) close() {
	tail.LastPosition, _ = tail.tell()
	close(tail.Lines)
}

func (tail *Tail) readLine() (string, error) {
	tail.lk.Lock()
	line, err := tail.reader.ReadString('\n')
	tail.lk.Unlock()
	if err != nil {
		// Note ReadString "returns the data read before the error" in
		// case of an error, including EOF, so we return it as is. The
		// caller is expected to process it if err is EOF.
		return line, err
	}

	line = strings.TrimRight(line, "\n")

	return line, err
}

func (tail *Tail) tailFileSync() error {
	defer tail.close()

	// Seek to requested location on first open of the file.
	if tail.Location != nil {
		_, err := tail.file.Seek(tail.Location.Offset, tail.Location.Whence)
		if err != nil {
			tail.Logger.Printf("[error] Seek failed %s - %+v, error: %v\n", tail.Filename, tail.Location, err)
			return fmt.Errorf("Seek error on %s: %s", tail.Filename, err)
		}
		tail.Logger.Printf("Seeked %s - %+v\n", tail.Filename, tail.Location)
	}

	tail.openReader()

	var offset int64 = 0
	var err error

	// Read line by line.
	for {
		// grab the position in case we need to back up in the event of a half-line
		offset, err = tail.tell()
		if err != nil {
			return err
		}

		line, err := tail.readLine()

		// Process `line` even if err is EOF.
		if err == nil {
			tail.sendLine(line)
		} else if err == io.EOF {
			if line != "" {
				// this has the potential to never return the last line if
				// it's not followed by a newline; seems a fair trade here
				err := tail.seekTo(SeekInfo{Offset: offset, Whence: 0})
				if err != nil {
					return err
				}
			}

			// When EOF is reached, wait for more data to become
			// available. Wait strategy is based on the `tail.watcher`
			// implementation (inotify or polling).
			err := tail.waitForChanges()
			if err != nil {
				if err != ErrStop {
					return err
				}
				return nil
			}
		} else {
			// non-EOF error
			return fmt.Errorf("Error reading %s: %s", tail.Filename, err)
		}

		select {
		case <-tail.Dying():
			return nil
		default:
		}
	}
}

// waitForChanges waits until the file has been appended, deleted,
// moved or truncated. When moved or deleted - the file will be
// reopened if ReOpen is true. Truncated files are always reopened.
func (tail *Tail) waitForChanges() error {
	pos, err := tail.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	event, err := tail.watcher.ChangeEvents(&tail.Tomb, pos)
	if err != nil {
		return err
	}

	switch event {
	case watch.Modified:
		return nil
	case watch.Deleted:
		tail.Logger.Printf("Stopping tail as file no longer exists: %s", tail.Filename)
		return ErrStop
	case watch.Unknow:
		return ErrStop
	}
	panic("unreachable")
}

func (tail *Tail) openReader() {
	tail.reader = bufio.NewReader(tail.file)
}

func (tail *Tail) seekEnd() error {
	return tail.seekTo(SeekInfo{Offset: 0, Whence: os.SEEK_END})
}

func (tail *Tail) seekTo(pos SeekInfo) error {
	_, err := tail.file.Seek(pos.Offset, pos.Whence)
	if err != nil {
		return fmt.Errorf("Seek error on %s: %s", tail.Filename, err)
	}
	// Reset the read buffer whenever the file is re-seek'ed
	tail.reader.Reset(tail.file)
	return nil
}

// sendLine sends the line(s) to Lines channel, splitting longer lines
// if necessary. Return false if rate limit is reached.
func (tail *Tail) sendLine(line string) {
	now := time.Now()
	tail.Lines <- &Line{line, now, nil}
}
