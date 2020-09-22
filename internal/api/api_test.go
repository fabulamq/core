package api

import (
	"bufio"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/zeusmq/internal/domain"
	"github.com/zeusmq/internal/services"
	"net"
	"os"
	"testing"
	"time"
)

func TestApi(t *testing.T) {
	services.Setup(services.Services{
		GetConsumers: nil,
		Write:        nil,
		OpenFile: func(ctx context.Context) (*os.File, error) {
			return &os.File{}, nil
		},
		WriteFile: func(ctx context.Context, f *os.File, b []byte) error {
			return nil
		},
		ReadLine: func(ctx context.Context, c net.Conn) string {
			scanner := bufio.NewScanner(c)
			scanner.Split(bufio.ScanLines)
			scanner.Scan()
			return scanner.Text()
		},
		SaveConsumerState:  nil,
		SaveFileToPreQueue: nil,
		SaveMsgToQueue:     nil,
		Ack:                nil,
		PublishStep:        nil,
	})
	ctx, cancel := context.WithCancel(context.Background())

	assert.NoError(t, domain.Start(ctx))
	assert.NoError(t, Start(ctx))

	conn1 := newConnection(t)
	fmt.Println("send ok")
	conn1.Write([]byte("{\"ID\":\"abcd\"}\n"))
	time.Sleep(30 * time.Second)
	cancel()
}

func newConnection(t *testing.T) net.Conn {
	conn, err := net.Dial("tcp", "localhost:9998")
	if err != nil {
		assert.NoError(t, err)
	}
	return conn
}
