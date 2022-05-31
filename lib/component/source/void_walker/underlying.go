package void_walker

import (
	"errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"time"
)

//打开一个文件的同时返回其 dev&&inode
func openFile(fn string) (f *os.File, identify *FileIdentify, err error) {
	f, err = os.Open(fn)
	if err != nil {
		return
	}

	identify = &FileIdentify{}
	identify.Device, identify.Inode, err = GetInode(f)
	if err != nil {
		f.Close()
		return
	}

	return
}

const OpenUntilAppearSleepInterval = time.Second * 1

//试图打开一个文件并返回dev&&inode,直到可以被打开.
//以 OPEN_UNTIL_APPEAR_INTERVAL 频率重试
func openFileUntilAppear(fn string, interval time.Duration, retryTimes int) (f *os.File, identify *FileIdentify, err error) {
	for ; retryTimes > 0; retryTimes-- {
		f, identify, err = openFile(fn)
		if err != nil {
			time.Sleep(interval)
			continue
		}
		return
	}
	return
}

var (
	ErrorCantFindFile = errors.New("can't find file")
)

//sure file in dir won't rename, file path
func findFile(dirPath string, dir []os.FileInfo, AccessLogPath string, targetIdentify *FileIdentify) (string, *os.File, error) {
	accessLogFile, accessLogIdentify, err := openFile(AccessLogPath)
	if err != nil {
		return "", nil, err
	}

	if accessLogIdentify.Same(targetIdentify) {
		return AccessLogPath, accessLogFile, nil
	}
	accessLogFile.Close()

	for _, fileInfo := range dir {
		fileID := NewFileIdentifyConvert(fileInfo.Sys().(*syscall.Stat_t))

		if targetIdentify.Same(fileID) {
			f, e := os.Open(path.Join(dirPath, fileInfo.Name()))
			return fileInfo.Name(), f, e
		}
	}

	return "", nil, ErrorCantFindFile
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

func removeFileByIdentify(dir string, id *FileIdentify) error {
	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, f := range fs {
		if id.Same(NewFileIdentifyConvert(f.Sys().(*syscall.Stat_t))) {
			logrus.Infoln("[vwul] re, moving file: ", path.Join(dir, f.Name()))
			//return os.Rename(path.Join(dir, f.Name()), path.Join(dir, f.Name() + ".ped"))
			return os.Remove(path.Join(dir, f.Name()))
		}
	}

	return errors.New("not found")
}

//从一个目录中,通过比对文件名找到当前文件的下一个文件,并返回其文件名
//如果当前文件不在目录中,返回ErrCantFindNowFile
//如果没有更新的文件出现,返回ErrNoNewerFile
//issue:
//	当文件的出现顺序与文件名顺序不一致时,按文件名顺序给出下一个文件
//	出现在时钟被向前拨动的时候
//	为何不按照创建时间排序:
//		创建时间同样受到时钟影响⏲️
func getNextFile(dirPath string, files []os.FileInfo, identify *FileIdentify) (newFn string, err error) {
	position := -1

	//sort.Sort(byModifyTime(dir))

	for i, f := range files {
		if identify.Same(NewFileIdentifyConvert(f.Sys().(*syscall.Stat_t))) {
			position = i
			break
		}
	}

	if position < 0 {
		return "", ErrCantFindNowFile
	} else if position == len(files)-1 {
		return "", ErrNoNewerFile
	}

	return path.Join(dirPath, files[position+1].Name()), nil
}
