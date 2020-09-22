package domain

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zeusmq/internal/services"
	"net"
	"testing"
	"time"
)

func TestProcessConsumer(t *testing.T) {
	services.Setup(services.Services{
		Write: func(ctx context.Context, c net.Conn, msg []byte) error {
			return nil
		},
	})
	res := ProcessConsumer(context.Background(), ProcessConsumerRequest{
		Status: func(status []ConsumerStatus) error {
			assert.Equal(t, uint64(500), status[0].Offset)
			return nil
		},
	})
	res.AddChannel("abc", &net.TCPConn{})
	assert.NoError(t, res.SendMessage(Message{
		Msg:    []byte("hello"),
		Offset: 500,
	}))
	res.Cancel()
	assert.EqualError(t, res.SendMessage(Message{
		Msg:    []byte("hello2"),
		Offset: 501,
	}), "context canceled")
	time.Sleep(1 * time.Second)
}
