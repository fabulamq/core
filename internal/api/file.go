package api

import (
	"context"
	"github.com/hpcloud/tail"
	"github.com/zeusmq/internal/infra/log"
	"os"
	"sync"
)

type file struct {
	file   *os.File
	m      sync.Mutex
	offset uint64
	once   sync.Once
}

func (f *file) createFile() {
	if _, err := os.Stat("/var/tmp/queue.txt"); os.IsNotExist(err) {
		var err error
		f.file, err = os.Create("/var/tmp/queue.txt")
		if err != nil {
			log.Fatal(context.Background(), err.Error())
		}
	}
}

func (f *file) AddOffset() {
	f.offset++
}

func (f *file) GetOffset() uint64 {
	f.once.Do(func() {
		f.file.Seek(0, 2)
	})
	return f.offset
}

func (f file) TailFile() (chan *tail.Line, error) {
	t, err := tail.TailFile("/var/tmp/queue.txt", tail.Config{Follow: true})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func (f file) OpenFile() (*os.File, error) {
	file, err := os.Open("/var/tmp/queue.txt")
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *file) WriteFile(ctx context.Context, b []byte) error {
	f.createFile()
	_, err := f.file.Write(append(b, []byte("\n")...))
	if err != nil {
		return err
	}
	return nil
}

func (f *file) CleanFile() {
	f.offset = 1
	os.Remove("/var/tmp/queue.txt")
	f.createFile()
}
