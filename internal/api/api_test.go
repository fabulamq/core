package api

import (
	"fmt"
	"github.com/fabulamq/go-fabula/pkg/gofabula"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// go test -v ./... -p 1 -count=1
// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race

var c *publisher
var status chan apiStatus

func setup() {
	dir, _ := os.Getwd()
	path := strings.Split(dir, "/fabulamq")[0] + "/fabulamq/.data"

	os.RemoveAll(path + "/")
	os.Mkdir(path, os.ModePerm)
	c, status = Start(Config{
		Host:             "localhost:9998",
		Folder:           path,
		OffsetPerChapter: 10,
	})
	<-status
	c.Reset()
}

var consumerOffset sync.Map

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race
func TestDifferentChannelConsumers(t *testing.T) {
	c.book.maxLinesPerChapter = uint64(50)
	{
		p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})
		go func() {
			for i := 0; i < 200; i++ {
				_, err := p.Write("topic-1", fmt.Sprintf("msg_%d\n testing second line to discard", i))
				assert.NoError(t, err)
			}
		}()
	}

	totalMsgConsumed := make(chan string)
	lastMsg := make(chan bool)

	go func() { // 160 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 3 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_1"
			return nil
		})
		lastMsg <- true
	}()
	go func() { // 50 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "2", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 0 && tail.Chapter == 1 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_2"
			return nil
		})
		lastMsg <- true
	}()

	go func() {// wait, 105 lines
		time.Sleep(1 * time.Second)
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 5 && tail.Chapter == 2 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_3"
			return nil
		})
	}()

	go func() { // only read 5 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter:2, Line:5}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 2 {
				assert.Equal(t, "msg_105", tail.Message)
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_4"
			return nil
		})
		lastMsg <- true
	}()

	totalMap := make(map[string]int)
	totalMsg := uint64(0)
	totalOut := 0
L:
	for {
		select {
		case <-lastMsg:
			totalOut++
			if totalOut == 4 {
				break L
			}
		case id := <-totalMsgConsumed:
			totalMap[id]++
			totalMsg++
		}
	}
	assert.Equal(t, 320, int(totalMsg))

}