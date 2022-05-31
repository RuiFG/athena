package void_walker

import (
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
	"time"
)

var (
	ErrCantFindNowFile = fmt.Errorf("can't find now file, not created yet, or removed unexpectedly")
	ErrNoNewerFile     = fmt.Errorf("no newer file")
)

//accesslog为access.log的位置,dir为rotate出来文件的位置. now为当前打开着的文件.
//返回下一个文件,不会关闭now
//当err返回ErrCantFindNowFile时,说明access.log还没有被rotate到dir下.
func (p *FileWalker) stepForward() (*FileWrapper, error) {
	files, err := ioutil.ReadDir(p.vwFolder)
	if err != nil {
		return nil, err
	}

	nextFilePath, e := getNextFile(p.vwFolder, files, p.current.FileIdentify)

	//if error is Can't find file, maybe someone removed access.log
	//to avoiding rotate slow, wait 20s, try 5 times here
	for retry := 0; e == ErrCantFindNowFile && retry < 5; retry++ {
		time.Sleep(time.Second * 4)
		nextFilePath, e = getNextFile(p.vwFolder, files, p.current.FileIdentify)
	}

	//if error is Can't find file after wait 20s, we think is someone removed file
	//just read fresh
	if e == ErrCantFindNowFile {
		fileAccessLog, idAccessLog, err := openFileUntilAppear(p.accessLogPath, OpenUntilAppearSleepInterval, int(time.Minute/OpenUntilAppearSleepInterval+1))
		if err != nil {
			//???
			return nil, err
		}
		return &FileWrapper{fileAccessLog, idAccessLog, p.accessLogPath}, nil
	}

	if e != nil && e != ErrNoNewerFile {
		return nil, e
	}

	if e == ErrNoNewerFile {
		//try to get access log
		fileAccessLog, idAccessLog, err := openFileUntilAppear(p.accessLogPath, OpenUntilAppearSleepInterval, int(time.Minute/OpenUntilAppearSleepInterval+1))
		if err != nil {
			//???
			return nil, err
		}

		//to avoid case:
		// before opened new accesslog, old accesslog had been rotated
		// so we check whether there some file between old file and now
		// opening file
		newFiles, err := ioutil.ReadDir(p.vwFolder)
		if err != nil {
			//???
			fileAccessLog.Close()
			return nil, err
		}

		newFilePath, err := getNextFile(p.vwFolder, newFiles, p.current.FileIdentify)
		if err == ErrNoNewerFile {
			//case ok
			return &FileWrapper{fileAccessLog, idAccessLog, p.accessLogPath}, nil
		}

		info, err := os.Lstat(newFilePath)
		if err != nil {
			//???
			fileAccessLog.Close()
			return nil, err
		}

		if idAccessLog.Same(NewFileIdentifyConvert(info.Sys().(*syscall.Stat_t))) {
			//case ok
			// : accesslog rotated, but no file between old file and now opening file
			return &FileWrapper{fileAccessLog, idAccessLog, newFilePath}, nil
		}

		//case bad
		//di gui yi xia
		fileAccessLog.Close()
		time.Sleep(time.Second)
		return p.stepForward()
	}

	//err == nil: next file in the void worker dir
	f, identify, err := openFile(nextFilePath)
	if err != nil {
		return nil, err
	}

	return &FileWrapper{f, identify, nextFilePath}, nil
}

type FileWrapper struct {
	*os.File
	*FileIdentify
	Path string
}

type FileWalker struct {
	current *FileWrapper

	vwFolder      string
	accessLogPath string

	first bool

	lock sync.Mutex
}

func NewFileWalker(accessLogPath string, identify *FileIdentify, VWFolder string) (fw *FileWalker, fresh bool, _err error) {
	filesInVWFolder, err := ioutil.ReadDir(VWFolder)
	if err != nil {
		return nil, false, err
	}

	filePath, file, err := findFile(VWFolder, filesInVWFolder, accessLogPath, identify)
	if err != nil {
		//reading fresh
		file, identify, err = openFileUntilAppear(accessLogPath, OpenUntilAppearSleepInterval, 32767)
		if err != nil {
			return nil, false, errors.WithMessage(err, "can't open access-log during init.")
		}

		fresh = true
		filePath = accessLogPath
	}

	return &FileWalker{
		current:       &FileWrapper{file, identify, filePath},
		vwFolder:      VWFolder,
		accessLogPath: accessLogPath,

		first: true,
		lock:  sync.Mutex{},
	}, fresh, nil
}

func (p *FileWalker) GetNextFile() *FileWrapper {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.first {
		p.first = false
		return p.current
	}

	p.current.File.Close()

	nextFile, err := p.stepForward()
	for err != nil {
		time.Sleep(time.Second)
		nextFile, err = p.stepForward()
	}

	p.current = nextFile
	return nextFile
}
