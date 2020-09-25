package services

import (
	"context"
	"github.com/hpcloud/tail"
	"io"
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
	Write              func(ctx context.Context, c io.Writer, msg []byte) error
	GetFile            func() (*os.File, error)
	TailFile           func() (chan *tail.Line, error)
	WriteFile          func(ctx context.Context, b []byte) error
	GetOffset          func() uint64
	AddOffset          func()
	ReadLine           func(ctx context.Context, c io.Reader) ([]byte, error)
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
