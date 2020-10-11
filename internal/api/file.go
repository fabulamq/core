package api

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/zeusmq/internal/infra/log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

type file struct {
	file   *os.File
	path   string
	m      sync.Mutex
	offset uint64
	once   sync.Once
}

func (f *file) getPath() string {
	return fmt.Sprintf("%s/%s", f.path, "queue.txt")
}

func (f *file) createFile() {
	if _, err := os.Stat(f.getPath()); os.IsNotExist(err) {
		var err error
		f.file, err = os.Create(f.getPath())
		if err != nil {
			log.Fatal(context.Background(), err.Error())
		}
	}
}

func (f *file) AddOffset() {
	atomic.AddUint64(&f.offset, 1)
	runtime.Gosched()
}

func (f *file) GetOffset() uint64 {
	f.once.Do(func() {
		f.file.Seek(0, 2)
	})
	return atomic.LoadUint64(&f.offset)
}

func (f file) TailFile(s *tail.SeekInfo) (chan *tail.Line, error) {
	t, err := tail.TailFile(f.getPath(), tail.Config{
		Follow:   true,
		Location: s,
	})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func (f file) OpenFile() (*os.File, error) {
	file, err := os.Open(f.getPath())
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *file) WriteFile(b []byte) error {
	f.createFile()
	_, err := f.file.Write(append(b, []byte("\n")...))
	if err != nil {
		return err
	}
	return nil
}

func (f *file) CleanFile() {
	f.offset = 1
	os.Remove(f.getPath())
	f.createFile()
}
