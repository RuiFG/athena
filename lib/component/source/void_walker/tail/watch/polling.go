// Copyright (c) 2015 HPE Software Inc. All rights reserved.
// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package watch

import (
	"os"
	"runtime"
	"time"

	"gopkg.in/tomb.v2"
	"syscall"
)

// PollingFileWatcher polls the file for changes.
type PollingFileWatcher struct {
	Filename string
	File     *os.File
}

func NewPollingFileWatcher(filename string, file *os.File) *PollingFileWatcher {
	fw := &PollingFileWatcher{filename, file}
	return fw
}

var POLL_DURATION time.Duration

func timespecToTime(ts syscall.Timespec) time.Time {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec))
}

//从一个已打开的文件上获取该文件的 dev&&inode
func GetInode(file *os.File) (dev uint64, ino uint64, err error) {
	var stat syscall.Stat_t

	fd := file.Fd()
	err = syscall.Fstat(int(fd), &stat)
	if err != nil {
		return 0, 0, err
	}

	//darwin's dev is int32
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

type Event int

const (
	Modified Event = iota
	Deleted
	Unknow
)

func (fw *PollingFileWatcher) ChangeEvents(t *tomb.Tomb, pos int64) (Event, error) {
	origDev, origIno, err := GetInode(fw.File)
	if err != nil {
		return Unknow, err
	}

	// XXX: use tomb.Tomb to cleanly manage these goroutines. replace
	// the fatal (below) with tomb's Kill.

	for {
		select {
		case <-t.Dying():
			return Unknow, nil
		default:
		}

		time.Sleep(POLL_DURATION)

		//获取文件更替
		//获取文件大小
		//比对文件大小，检测是否有大小改变
		//比对文件dev&&inode，检测文件是否更替

		//如此设计是为了防止在获取完大小改变且发现大小没有改变的情况下，
		//文件发生改变且发生更替的情况的发生。如此一来的话只能发现更替，而检测不到大小变化
		//最后一行会被丢弃，有可能。

		//其中文件大小变化只能由syscall.Fstat对已打开文件的信息获取来完成

		fi, err2 := os.Stat(fw.Filename)

		size, _, err := getInfo(fw.File)
		if err != nil {

			return Unknow, err
		}

		// File got bigger?
		if pos < size {
			return Modified, nil
		} else if pos > size { //case unknow
			return Unknow, err
		}

		if err2 != nil {
			// Windows cannot delete a file if a handle is still open (tail keeps one open)
			// so it gives access denied to anything trying to read it until all handles are released.
			if os.IsNotExist(err2) || (runtime.GOOS == "windows" && os.IsPermission(err2)) {
				// File does not exist (has been deleted).

				return Deleted, nil
			}

			return Unknow, err2
		}

		// File got moved/renamed?
		if uint64(fi.Sys().(*syscall.Stat_t).Ino) != origIno ||
			uint64(fi.Sys().(*syscall.Stat_t).Dev) != origDev {

			return Deleted, nil
		}

		//logrus.Infoln(origIno, origDev, fi.Sys().(*syscall.Stat_t).Ino, fi.Sys().(*syscall.Stat_t).Dev)
	}
}

func init() {
	POLL_DURATION = 250 * time.Millisecond
}
