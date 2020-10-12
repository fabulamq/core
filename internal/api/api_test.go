package api

import (
	"fmt"
	"github.com/go-zeusmq/pkg/gozeusmq"
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
	os.RemoveAll("/home/kanczuk/go/src/github.com/zeusmq/.data/")
	os.Mkdir("/home/kanczuk/go/src/github.com/zeusmq/.data", os.ModePerm)
	c, status = Start(Config{
		Host:             "localhost:9998",
		Folder:           "/home/kanczuk/go/src/github.com/zeusmq/.data",
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

// fabulaMq

func TestDifferentChannelConsumers(t *testing.T) {
	c.book.maxLinesPerChapter = int64(50)
	{
		p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
		go func() {
			for i := 0; i < 200; i++ {
				_, err := p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
				assert.NoError(t, err)
			}
		}()
	}

	totalMsgConsumed := make(chan string)
	lastMsg := make(chan bool)

	go func() { // 160 lines
		cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Mark: gozeusmq.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		cli1.Handle(func(req gozeusmq.ZeusRequest) error {
			if req.Line == 10 && req.Chapter == 3 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_1"
			return nil
		})
		lastMsg <- true
	}()
	go func() { // 50 lines
		cli2, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "2", Mark: gozeusmq.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		cli2.Handle(func(req gozeusmq.ZeusRequest) error {
			if req.Line == 0 && req.Chapter == 1 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_2"
			return nil
		})
		lastMsg <- true
	}()

	go func() {// wait, 105 lines
		time.Sleep(1 * time.Second)
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "3", Mark: gozeusmq.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			if req.Line == 5 && req.Chapter == 2 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_3"
			return nil
		})
		lastMsg <- true
	}()

	go func() { // only read 5 lines
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "3", Mark: gozeusmq.Mark{Chapter:2, Line:5}, Host: "localhost:9998"})
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			if req.Line == 10 && req.Chapter == 2 {
				assert.Equal(t, "msg_105", req.Message)
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

func TestFromAheadOffset(t *testing.T) {
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

	for i := 1; i <= 200; i++ {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
	}
	cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Mark: gozeusmq.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
	cli1.Handle(func(req gozeusmq.ZeusRequest) error {
		if req.Line == 200 {
			return fmt.Errorf("error")
		}
		return nil
	})
}

func TestStrategyCustomOffset(t *testing.T) {
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

	for i := 1; i <= 10; i++ {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
	}

	lastMsgReceived := ""

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Mark: gozeusmq.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		p.Produce("topic-1", "myLastMessage")
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			lastMsgReceived = req.Message
			return fmt.Errorf("error")
		})
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, "myLastMessage", lastMsgReceived)
}

func TestSyncProducer(t *testing.T) {
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

	for i := 1; i <= 10; i++ {
		msgId, _ := p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		s, _ := gozeusmq.NewSync(gozeusmq.ConfigS{
			Host:  "localhost:9998",
			MsgId: msgId,
		})
		s.Sync(func(message gozeusmq.SyncMessage) bool {
			return true
		})
	}

	lastMsgReceived := ""

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Mark: gozeusmq.Mark{Chapter:0, Line:0}, Host: "localhost:9998"})
		p.Produce("topic-1", "myLastMessage")
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			lastMsgReceived = req.Message
			return fmt.Errorf("error")
		})
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, "myLastMessage", lastMsgReceived)
}
