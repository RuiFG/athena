package void_walker

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"syscall"
)

type RowIdentify struct {
	*FileIdentify
	Position int64
}

type FileIdentify struct {
	Device uint64
	Inode  uint64
}

func (f *FileIdentify) Same(other *FileIdentify) bool {
	return f.Device == other.Device && f.Inode == other.Inode
}

func NewFileIdentifyConvert(stat *syscall.Stat_t) *FileIdentify {
	return &FileIdentify{Device: uint64(stat.Dev), Inode: stat.Ino}
}

func restorePosition(path string, id *RowIdentify) error {
	return retryStoreLastPosition(path, id, 10)
}

func loadLastPositions(fn string) *RowIdentify {
	bs, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil
	}

	result := RowIdentify{}
	err = json.Unmarshal(bs, &result)
	if err != nil {
		return nil
	}

	return &result
}

func storeLastPositions(fn string, lst *RowIdentify) error {
	bs, err := json.Marshal(lst)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fn, bs, 0644)
}

func retryStoreLastPosition(fn string, lst *RowIdentify, retry int) error {
	var err error

	for i := 0; i < retry; i++ {
		err = storeLastPositions(fn, lst)
		if err != nil {
			continue
		}
		return nil
	}
	return errors.WithMessage(err, "can't store last position, retry end")
}
