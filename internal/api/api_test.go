package api

import (
	"fmt"
	"github.com/go-zeusmq/pkg/gozeusmq"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// go test -v ./... -p 1 -count=1
// go test -v ./... -p 1 -count=10000 -run TestApiErrorOnConsumer -failfast

var c *Controller
var status chan apiStatus

func setup() {
	c, status = Start()
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

func TestApi(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id1", Ch: "ch1", Topic: "topic-1"})
	go func() {
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			consumerOffset.Store("id1", int64(req.Offset))
			if req.Offset == 5 {
				wg.Done()
				return fmt.Errorf("error")
			}
			return nil
		})
	}()
	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
	i := 0
	for {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		if i == 5 {
			break
		}
		i++
	}
	wg.Wait()
	c.file.CleanFile()
}

// TODO resolver este caso com connection pedindo offset de mensagem... dificil
func TestApiErrorOnConsumer(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id2", Ch: "ch1", Topic: "topic-1"})
	go func() {
		cli1.Handle(func(req gozeusmq.ZeusRequest) error {
			consumerOffset.Store("id2", int64(req.Offset))
			if req.Offset != 0 && req.Offset%2 == 0 {
				return fmt.Errorf("error")
			}
			return nil
		})
		wg.Done()
	}()

	//cli2, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id3", Ch: "ch1", Topic: "topic-1"})
	//go func() {
	//	cli2.Handle(func(req gozeusmq.ZeusRequest) error {
	//		consumerOffset.Store("id3", int64(req.Offset))
	//		if req.Offset == 8 {
	//			fmt.Println("PASSOU ID3")
	//			wg.Done()
	//			return fmt.Errorf("error")
	//		}
	//		return nil
	//	})
	//}()
	//
	//cli3, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id4", Ch: "ch1", Topic: "topic-2"})
	//go func() {
	//	cli3.Handle(func(req gozeusmq.ZeusRequest) error {
	//		fmt.Println("PASSOU ID4")
	//		consumerOffset.Store("id4", int64(req.Offset))
	//		wg.Done()
	//		return fmt.Errorf("error here")
	//	})
	//}()

	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
	i := 0
	for {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		if i == 5 {
			//p.Produce("topic-2", fmt.Sprintf("msg_%d", i))
			break
		}
		i++
	}

	wg.Wait()

	id2Value, _ := consumerOffset.Load("id2")
	//id3Value, _ := consumerOffset.Load("id3")
	//id4Value, _ := consumerOffset.Load("id4")
	assert.Equal(t, id2Value, int64(2))
	//assert.Equal(t, id3Value, int64(3))
	//assert.Equal(t, id4Value, int64(6))
	c.file.CleanFile()

}

func TestReadingFromBegining(t *testing.T) {
	c.file.CleanFile()
	total := 1000000000
	go func() {
		p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
		i := 0
		for {
			p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
			if i == total {
				break
			}
			i++
			//time.Sleep(200 * time.Millisecond)
		}
	}()
	//go func() {
	//	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
	//	i := 0
	//	for {
	//		p.Produce("topic-2", fmt.Sprintf("msg_%d", i))
	//		if i == total {
	//			break
	//		}
	//		i++
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}()

	var wg sync.WaitGroup
	wg.Add(1)

	k := 0
	for {
		k++
		time.Sleep(1 * time.Second)
		go func(n int) {
			cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: fmt.Sprintf("idd_%d", n), Ch: "ch1", Topic: "topic-1"})
			cli.Handle(func(req gozeusmq.ZeusRequest) error {
				if rand.Intn(100) > 99 {
					return fmt.Errorf("error")
				}
				return nil
			})
		}(k)
	}

	wg.Wait()
	//file.CleanFile()
}

func TestDifferentChannelConsumers(t *testing.T) {
	go func() {
		p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})

		for i := 1; i <= 20000; i++ {
			p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		}
	}()
	totalMsgConsumed := make(chan string)
	lastMsg := make(chan bool)

	go func() {
		cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Strategy: gozeusmq.FromStart, Host: "localhost:9998", ID: "id_1", Ch: "ch1", Topic: "topic-1"})
		cli1.Handle(func(req gozeusmq.ZeusRequest) error {
			totalMsgConsumed <- "ch_1"
			if req.Message == "msg_20000" {
				lastMsg <- true
			}
			return nil
		})
	}()
	go func() {
		cli2, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Strategy: gozeusmq.FromStart, Host: "localhost:9998", ID: "id_1", Ch: "ch2", Topic: "topic-1"})
		cli2.Handle(func(req gozeusmq.ZeusRequest) error {
			totalMsgConsumed <- "ch_2"
			if req.Message == "msg_20000" {
				lastMsg <- true
			}
			return nil
		})
	}()

	go func() {
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Strategy: gozeusmq.FromStart, Host: "localhost:9998", ID: "id_1", Ch: "ch3", Topic: "topic-1"})
		cli.Handle(func(req gozeusmq.ZeusRequest) error {
			totalMsgConsumed <- "ch_3"
			if req.Message == "msg_20000" {
				lastMsg <- true
			}
			return nil
		})
	}()

	totalMap := make(map[string]int)
	totalMsg := 0
L:
	for {
		select {
		case <-lastMsg:
			break L
		case id := <-totalMsgConsumed:
			totalMap[id]++
			totalMsg++
		}
	}
	assert.Equal(t, 20000, totalMsg)
	c.file.CleanFile()
	c.Reset()
	fmt.Println(totalMap)

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
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Strategy: gozeusmq.FromCurrent, Host: "localhost:9998", ID: "id_1", Ch: "ch1", Topic: "topic-1"})
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
