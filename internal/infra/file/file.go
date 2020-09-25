package file

import (
	"context"
	"github.com/hpcloud/tail"
	"github.com/zeusmq/internal/infra/log"
	"os"
	"sync"
)

var once = sync.Once{}
var file *os.File
var m sync.Mutex
var offset uint64

func createFile() {
	if _, err := os.Stat("/var/tmp/queue.txt"); os.IsNotExist(err) {
		var err error
		file, err = os.Create("/var/tmp/queue.txt")
		if err != nil {
			log.Fatal(context.Background(), err.Error())
		}
	}
}

func AddOffset() {
	offset++
}

func GetOffset() uint64 {
	return offset
}

func TailFile() (chan *tail.Line, error) {
	t, err := tail.TailFile("/var/tmp/queue.txt", tail.Config{Follow: true})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func OpenFile() (*os.File, error) {
	file, err := os.Open("/var/tmp/queue.txt")
	if err != nil {
		return nil, err
	}
	return file, nil
}

func WriteFile(ctx context.Context, b []byte) error {
	_, err := file.Write(append(b, []byte("\n")...))
	if err != nil {
		return err
	}
	return nil
}

func CleanFile() {
	offset = 0
	os.Remove("/var/tmp/queue.txt")
	createFile()
}
