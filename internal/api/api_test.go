package api

import (
	"fmt"
	"github.com/go-fabula/pkg/gofabula"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

// go test -v ./... -p 1 -count=1
// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race
// go test -v ./... -run TestDifferentChannelConsumers -failfast -race

var c *Controller
var status chan apiStatus

func setup() {
	path, _ := os.Getwd()
	fmt.Println(path)
	os.RemoveAll("/home/kanczuk/go/src/github.com/fabulamq/.data/")
	os.Mkdir("/home/kanczuk/go/src/github.com/fabulamq/.data", os.ModePerm)
	c, status = Start(Config{
		Host:             "localhost:9998",
		Folder:           "/home/kanczuk/go/src/github.com/fabulamq/.data",
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

func TestDifferentChannelConsumers(t *testing.T) {
	c.book.maxLinesPerChapter = int64(50)
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
		cli1, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		cli1.Read(func(line gofabula.FabulaLine) error {
			if line.Line == 10 && line.Chapter == 3 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_1"
			return nil
		})
		lastMsg <- true
	}()
	go func() { // 50 lines
		cli2, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "2", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		cli2.Read(func(line gofabula.FabulaLine) error {
			if line.Line == 0 && line.Chapter == 1 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_2"
			return nil
		})
		lastMsg <- true
	}()

	go func() {// wait, 105 lines
		time.Sleep(1 * time.Second)
		cli, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		cli.Read(func(line gofabula.FabulaLine) error {
			if line.Line == 5 && line.Chapter == 2 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_3"
			return nil
		})
		lastMsg <- true
	}()

	go func() { // only read 5 lines
		cli, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter:2, Line:5}, Host: "localhost:9998"})
		cli.Read(func(line gofabula.FabulaLine) error {
			if line.Line == 10 && line.Chapter == 2 {
				assert.Equal(t, "msg_105", line.Message)
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

func TestSyncProducer(t *testing.T) {
	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})

	mark, _ := p.Write("topic-1", fmt.Sprintf("msg_%d", 0))

	cli, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})

	var wg sync.WaitGroup
	wg.Add(2)

	s, _ := gofabula.NewSync(gofabula.ConfigS{
		Host: "localhost:9998",
		Mark: mark,
	})
	go func() {
		s.Sync(func(message gofabula.SyncMessage) bool {
			fmt.Println(message.ConsumerID)
			return true
		})
		wg.Done()
	}()

	lastMsgReceived := ""

	go func() {
		p.Write("topic-1", "myLastMessage")
		cli.Read(func(line gofabula.FabulaLine) error {
			lastMsgReceived = line.Message
			return fmt.Errorf("error")
		})
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, "myLastMessage", lastMsgReceived)
}
