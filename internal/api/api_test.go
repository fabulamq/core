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
	"testing"
)

func setup(t *testing.T) func() {
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

	err, isReady := Start(ctx)
	assert.NoError(t, err)
	<-isReady
	return cancel
}

func TestApi(t *testing.T) {
	setup(t)

	go func() {
		i := 0
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id1", Ch: "ch1", Topic: "topic-1"})
		err := cli.Handle(func(req gozeusmq.ZeusRequest) error {
			assert.Equal(t, fmt.Sprintf("msg_%d", i), req.Message)
			i++
			return nil
		})
		fmt.Println(err)
	}()

	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
	i := 0
	for {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		if i == 20 {
			break
		}
		i++
	}
}

func TestApi2(t *testing.T) {
	setup(t)

	go func() {
		i := 0
		cli, _ := gozeusmq.NewConsumer(gozeusmq.ConfigC{Host: "localhost:9998", ID: "id1", Ch: "ch1", Topic: "topic-1"})
		err := cli.Handle(func(req gozeusmq.ZeusRequest) error {
			assert.Equal(t, fmt.Sprintf("msg_%d", i), req.Message)
			i++
			if req.Message == "msg_10" {
				return fmt.Errorf("error")
			}
			return nil
		})
		fmt.Println(err)
	}()

	p, _ := gozeusmq.NewProducer(gozeusmq.ConfigP{Host: "localhost:9998"})
	i := 0
	for {
		p.Produce("topic-1", fmt.Sprintf("msg_%d", i))
		if i == 20 {
			break
		}
		i++
	}
}
