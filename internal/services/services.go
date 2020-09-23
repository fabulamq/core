package services

import (
	"context"
	"net"
	"os"
)

var s Services

func Setup(c Services) {
	s = c
}

func Get() Services {
	return s
}

type Services struct {
	GetConsumers       func(ctx context.Context) ([]ConsumerResponse, error)
	Write              func(ctx context.Context, c net.Conn, msg []byte) error
	OpenFile           func(ctx context.Context) (*os.File, error)
	WriteFile          func(ctx context.Context, f *os.File, b []byte) error
	ReadLine           func(ctx context.Context, c net.Conn) (string, error)
	SaveConsumerState  func(ctx context.Context, req ConsumerStateRequest) error
	SaveFileToPreQueue func(ctx context.Context, msg string) error
	SaveMsgToQueue     func(ctx context.Context, req SaveMsgRequest) (SaveMsgResponse, error)
	Ack                func(ctx context.Context, c net.Conn) error
	PublishStep        func(ctx context.Context, req SaveMsgResponse) error
}

type SaveMsgRequest struct {
	Msg    string
	Topics string
}

type SaveMsgResponse struct {
	Offset uint64
	Msg    string
	Topics string
}

type ConsumerResponse struct {
	Name       string
	Topics     []string
	Channels   []net.Conn
	LastMsgIDX uint64
}

func (r ConsumerResponse) HasTopic(topic string) bool {
	for _, t := range r.Topics {
		if t == topic {
			return true
		}
	}
	return false
}

type ConsumerStateRequest struct {
	Name   string
	Offset uint64
}
