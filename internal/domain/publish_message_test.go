package domain

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/services"
	"net"
	"testing"
	"time"
)

func TestPublishMessage(t *testing.T) {
	services.Setup(services.Services{
		GetConsumers: func(ctx context.Context) ([]services.ConsumerResponse, error) {
			return []services.ConsumerResponse{
				{Name: "consumer1", Topics: []string{"TOPIC_1", "TOPIC_2"}, Channels: []net.Conn{&net.TCPConn{}, &net.TCPConn{}}},
				{Name: "consumer2", Topics: []string{"TOPIC_1", "TOPIC_2"}, Channels: []net.Conn{&net.TCPConn{}, &net.TCPConn{}}},
				{Name: "consumer3", Topics: []string{"TOPIC_1"}, Channels: []net.Conn{&net.TCPConn{}, &net.TCPConn{}}},
			}, nil
		},
		Write: func(ctx context.Context, c net.Conn, msg []byte) error {
			return nil
		},
		ReadLine: func(ctx context.Context, c net.Conn) string {
			time.Sleep(1 * time.Second)
			return "ok"
		},
		SaveConsumerState: func(ctx context.Context, req services.ConsumerStateRequest) error {
			return nil
		},
	})
	res, err := PubMessage(context.Background(), PublishMessageRequest{
		Msg:    "hello",
		Topic:  "TOPIC_2",
		Offset: 200,
	})

	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(res)
}
