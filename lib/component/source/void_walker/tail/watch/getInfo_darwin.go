//go:build darwin
// +build darwin

package watch

import (
	"os"
	"syscall"
	"time"
)

func getInfo(file *os.File) (size int64, modTime time.Time, err error) {
	var stat syscall.Stat_t

	fd := file.Fd()
	err = syscall.Fstat(int(fd), &stat)
	if err != nil {
		return 0, time.Time{}, err
	}

	return stat.Size, timespecToTime(stat.Mtimespec), nil
}
