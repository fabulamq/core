package api

import (
	"context"
	"fmt"
	"github.com/go-zeusmq/pkg/gozeusmq"
	"github.com/stretchr/testify/assert"
	"github.com/zeusmq/internal/infra/connection"
	"github.com/zeusmq/internal/services"
	"net"
	"os"
	"sync"
	"testing"
)

// go test -v ./... -p 1 -count=1

func setup() func() {
	services.Setup(services.Services{
		GetConsumers: nil,
		Write: func(ctx context.Context, c net.Conn, msg []byte) error {
			_, err := c.Write(msg)
			return err
		},
		OpenFile: func(ctx context.Context) (*os.File, error) {
			return &os.File{}, nil
		},
		WriteFile: func(ctx context.Context, f *os.File, b []byte) error {
			return nil
		},
		ReadLine: connection.ReadLine,
	})
	ctx, cancel := context.WithCancel(context.Background())

	_, isReady := Start(ctx)
	<-isReady
	return cancel
}

var consumerOffset sync.Map

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func NonTestApi(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id1", Ch: "ch1", Topic: "topic-1"})
	go func() {
		err := cli.Handle(func(req gozeusmq.ZeusRequest) error {
			consumerOffset.Store("id1", int64(req.Offset))
			if req.Offset == 5 {
				wg.Done()
			}
			return nil
		})
		fmt.Println(err)
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
	cli.Close()
}

func TestApiErrorOnConsumer(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(3)

	cli1, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id2", Ch: "ch1", Topic: "topic-1"})
	go func() {
		err := cli1.Handle(func(req gozeusmq.ZeusRequest) error {
			consumerOffset.Store("id2", int64(req.Offset))
			if req.Offset == 5 {
				fmt.Println("PASSOU ID2")
				wg.Done()
			}
			return nil
		})
		fmt.Println(err)
	}()

	cli2, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id3", Ch: "ch1", Topic: "topic-1"})
	go func() {
		err := cli2.Handle(func(req gozeusmq.ZeusRequest) error {
			consumerOffset.Store("id3", int64(req.Offset))
			if req.Offset == 3 {
				fmt.Println("PASSOU ID3")
				wg.Done()
				return fmt.Errorf("error")
			}
			return nil
		})
		fmt.Println(err)
	}()

	cli3, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id4", Ch: "ch1", Topic: "topic-2"})
	go func() {
		err := cli3.Handle(func(req gozeusmq.ZeusRequest) error {
			fmt.Println("PASSOU ID4")
			consumerOffset.Store("id4", int64(req.Offset))
			wg.Done()
			return nil
		})
		fmt.Println(err)
	}()

	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
	i := 0
	for {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		if i == 5 {
			p.Produce("topic-2", fmt.Sprintf("msg_%d", i))
			break
		}
		i++
	}

	wg.Wait()
	cli1.Close()
	cli2.Close()
	cli3.Close()

	id2Value, _ := consumerOffset.Load("id2")
	id3Value, _ := consumerOffset.Load("id3")
	id4Value, _ := consumerOffset.Load("id4")
	assert.Equal(t, id2Value, int64(5))
	assert.Equal(t, id3Value, int64(3))
	assert.Equal(t, id4Value, int64(6))

}
