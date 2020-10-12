package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/internal/infra/log"
	"github.com/hpcloud/tail"
	"os"
	"sync"
)

type file struct {
	file   *os.File
	path   string
	m      sync.Mutex
	offset uint64
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
