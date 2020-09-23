package api

import (
	"bufio"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/zeusmq/internal/services"
	"log"
	"net"
	"os"
	"testing"
)

func TestApi(t *testing.T) {
	services.Setup(services.Services{
		GetConsumers: nil,
		Write: func(ctx context.Context, c net.Conn, msg []byte) error {
			log.Println("test.Write: ", string(msg))
			_, err := c.Write(msg)
			return err
		},
		OpenFile: func(ctx context.Context) (*os.File, error) {
			return &os.File{}, nil
		},
		WriteFile: func(ctx context.Context, f *os.File, b []byte) error {
			log.Println("test.WriteFile")
			return nil
		},
		ReadLine: func(ctx context.Context, c net.Conn) (string, error) {
			scanner := bufio.NewScanner(c)
			scanner.Split(bufio.ScanLines)
			if scanner.Scan() {
				return scanner.Text(), nil
			}
			return "", fmt.Errorf("connection closed")
		},
	})
	ctx, cancel := context.WithCancel(context.Background())

	err, isReady := Start(ctx)
	assert.NoError(t, err)
	<-isReady

	conn1 := newConnection(t)
	conn2 := newConnection(t)
	conn1.Write([]byte("{\"ID\":\"id1\", \"Ch\":\"ch1\"}\n"))
	conn2.Write([]byte("{\"ID\":\"id2\", \"Ch\":\"ch2\"}\n"))
	services.Get().ReadLine(ctx, conn1)
	services.Get().ReadLine(ctx, conn2)

	_, err = conn1.Write([]byte("{\"Topic\":\"topic-1\",\"Message\":\"hello\"}\n"))
	assert.NoError(t, err)
	{
		txt, _ := services.Get().ReadLine(ctx, conn1)
		fmt.Println("final text: ", txt)
	}
	{
		txt, _ := services.Get().ReadLine(ctx, conn2)
		fmt.Println("final text: ", txt)
	}
	cancel()
}

func newConnection(t *testing.T) net.Conn {
	conn, err := net.Dial("tcp", "localhost:9998")
	if err != nil {
		assert.NoError(t, err)
	}
	return conn
}
