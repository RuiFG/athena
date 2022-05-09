package spooldir

import (
	"os"
	"syscall"
)

type Identify struct {
	Device uint64
	Inode  uint64
}

func (f Identify) Same(other Identify) bool {
	return f.Device == other.Device && f.Inode == other.Inode
}

func convertPathToIdentify(filePath string) (Identify, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return Identify{}, err
	}
	var stat syscall.Stat_t

	fd := file.Fd()
	err = syscall.Fstat(int(fd), &stat)
	if err != nil {
		return Identify{}, err
	}
	return Identify{Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

func convertStatToIdentify(stat *syscall.Stat_t) Identify {
	return Identify{Device: uint64(stat.Dev), Inode: stat.Ino}
}
