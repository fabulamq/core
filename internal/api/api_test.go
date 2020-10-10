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
	c, status = Start(Config{
		Host:             "localhost:9998",
		OffsetPerChapter: 1000,
	})
	c.file.CleanFile()
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
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

	go func() {
		for i := 1; i <= 20000; i++ {
			_, err := p.Produce("group-1", fmt.Sprintf("msg_%d", i))
			assert.NoError(t, err)
		}
	}()
	totalMsgConsumed := make(chan string)
	lastMsg := make(chan bool)

	go func() {
		cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Offset: gozeusmq.OffsetStrategy{}, Host: "localhost:9998", Group: "group-1"})
		cli1.Handle(func(req gozeusmq.ZeusRequest) error {
			totalMsgConsumed <- "ch_1"
			if req.Offset == 10000 {
				return fmt.Errorf("error")
			}
			return nil
		})
		lastMsg <- true
	}()
	go func() {
		cli2, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "2", Offset: gozeusmq.OffsetStrategy{}, Host: "localhost:9998", Group: "group-1"})
		cli2.Handle(func(req gozeusmq.ZeusRequest) error {
			totalMsgConsumed <- "ch_2"
			if req.Offset == 15000 {
				return fmt.Errorf("error")
			}
			return nil
		})
		lastMsg <- true
	}()

	go func() {
		time.Sleep(1 * time.Second)
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "3", Offset: gozeusmq.OffsetStrategy{}, Host: "localhost:9998", Group: "group-1"})
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			totalMsgConsumed <- "ch_3"
			if req.Offset == 18000 {
				return fmt.Errorf("error")
			}
			return nil
		})
		lastMsg <- true
	}()

	totalMap := make(map[string]int)
	totalMsg := 0
	totalOut := 0
L:
	for {
		select {
		case <-lastMsg:
			totalOut++
			if totalOut == 3 {
				break L
			}
		case id := <-totalMsgConsumed:
			totalMap[id]++
			totalMsg++
		}
	}
	assert.Equal(t, 43000, totalMsg)

}

func TestFromAheadOffset(t *testing.T) {
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

	for i := 1; i <= 200; i++ {
		p.Produce("group-1", fmt.Sprintf("msg_%d", i))
	}
	cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Offset: gozeusmq.OffsetStrategy{Chapter: 0, Offset:  0}, Host: "localhost:9998", Group: "group-1"})
	cli1.Handle(func(req gozeusmq.ZeusRequest) error {
		if req.Offset == 200 {
			return fmt.Errorf("error")
		}
		return nil
	})
}

func TestStrategyCustomOffset(t *testing.T) {
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

	for i := 1; i <= 10; i++ {
		p.Produce("group-1", fmt.Sprintf("msg_%d", i))
	}

	lastMsgReceived := ""

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Offset: gozeusmq.OffsetStrategy{}, Host: "localhost:9998", Group: "group-1"})
		p.Produce("group-1", "myLastMessage")
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
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{ID: "1", Offset: gozeusmq.OffsetStrategy{}, Host: "localhost:9998", Group: "group-1"})
		p.Produce("group-1", "myLastMessage")
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			lastMsgReceived = req.Message
			return fmt.Errorf("error")
		})
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, "myLastMessage", lastMsgReceived)
}