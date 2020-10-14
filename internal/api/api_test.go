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
		conf := gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"}
		gofabula.NewStoryReader(conf, func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 3 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_1"
			return nil
		})
		lastMsg <- true
	}()
	go func() { // 50 lines
		conf := gofabula.ConfigReader{ID: "2", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"}
		gofabula.NewStoryReader(conf, func(tail gofabula.FabulaTail) error {
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
		conf := gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"}
		readerCh := gofabula.NewStoryReader(conf, func(tail gofabula.FabulaTail) error {
			if tail.Line == 5 && tail.Chapter == 2 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_3"
			return nil
		})
		L: for {
			select {
			case rs := <- readerCh:
				if rs.Status == gofabula.Forced {
					break L
				}
			}
		}
		lastMsg <- true
	}()

	go func() { // only read 5 lines
		conf := gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter:2, Line:5}, Host: "localhost:9998"}
		gofabula.NewStoryReader(conf, func(tail gofabula.FabulaTail) error {
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

func TestAuditorWithOneMessage(t *testing.T) {
	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})

	mark, _ := p.Write("topic-1", fmt.Sprintf("msg_%d", 0))

	waitSync := make(chan bool, 1)

	gofabula.StartSyncMessage(gofabula.ConfigS{Host: "localhost:9998", Mark: mark},func(message gofabula.SyncMessage) bool {
		if message.ConsumerID == "1" && message.Status == gofabula.ReadIt {
			waitSync <- true
			return false
		}
		return true
	})

	conf := gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"}
	gofabula.NewStoryReader(conf, func(r gofabula.FabulaTail) error {
		return nil
	})

	<-waitSync
}

// go test -v ./...  -count=1 -run TestAuditorWithTwoChapters -failfast -race
func TestAuditorWithTwoChapters(t *testing.T) {
	c.book.maxLinesPerChapter = uint64(5)
	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})

	mark := &gofabula.Mark{
		Chapter: 2,
		Line:    0,
	}
	for i := 0; i <= 15; i++ {
		p.Write("topic-1", fmt.Sprintf("msg_%d", i))
	}

	readCh := make(chan bool, 1)
	almostCh := make(chan bool, 1)
	farAwayCh := make(chan bool, 1)
	cantReadCh := make(chan bool, 1)

	syncStatus := gofabula.StartSyncMessage(gofabula.ConfigS{Host: "localhost:9998", Mark: mark},func(message gofabula.SyncMessage) bool {
		if message.ConsumerID == "1" && message.Status == gofabula.FarAway {
			farAwayCh <- true
		}
		if message.ConsumerID == "1" && message.Status == gofabula.Almost {
			almostCh <- true
		}
		if message.ConsumerID == "1" && message.Status == gofabula.ReadIt {
			readCh <- true
		}
		if message.ConsumerID == "1" && message.Status == gofabula.CantRead {
			cantReadCh <- true
			return false
		}
		return true
	})

	<-syncStatus

	conf := gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter:0, Line:0}, Host: "localhost:9998"}
	gofabula.NewStoryReader(conf, func(tail gofabula.FabulaTail) error {
		if tail.Message == "lastMessage" {
			return fmt.Errorf("error reading last message")
		}
		return nil
	})

	<- farAwayCh
	<- almostCh
	<- readCh

	p.Write("topic-1", "lastMessage")

	<- cantReadCh
}
