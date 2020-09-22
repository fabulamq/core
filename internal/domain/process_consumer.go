package domain

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/services"
	"net"
)

type Message struct {
	Msg    []byte
	Offset uint64
}

type ProcessConsumerRequest struct {
	Status func([]ConsumerStatus) error
}

type ProcessConsumerResponse struct {
	message     chan Message
	ctx         context.Context
	consumerMap chan consumer
	cancel      func()
}

func (p ProcessConsumerResponse) AddChannel(name string, conn net.Conn) {
	p.consumerMap <- consumer{
		conn: conn,
		Name: name,
	}
}

func (p ProcessConsumerResponse) SendMessage(msg Message) error {
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}
	p.message <- msg
	return nil
}
func (p ProcessConsumerResponse) Cancel() {
	p.cancel()
}

type consumer struct {
	conn net.Conn
	Name string
}

type ConsumerStatus struct {
	Channel string
	Offset  uint64
}

func ProcessConsumer(ctx context.Context, req ProcessConsumerRequest) ProcessConsumerResponse {
	chanMsg := make(chan Message)
	consumerChan := make(chan consumer)
	consumerMap := make(map[string]net.Conn)

	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("ctx.Done()")
				return
			case message := <-chanMsg:
				consumersStatus := make([]ConsumerStatus, 0)
				for channel, conn := range consumerMap {
					err := services.Get().Write(ctx, conn, message.Msg)
					if err != nil {
						continue
					}
					consumersStatus = append(consumersStatus, ConsumerStatus{
						Channel: channel,
						Offset:  message.Offset,
					})
				}
				req.Status(consumersStatus)

			case cc := <-consumerChan:
				consumerMap[cc.Name] = cc.conn
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	return ProcessConsumerResponse{
		message:     chanMsg,
		ctx:         ctx,
		consumerMap: consumerChan,
		cancel:      cancel,
	}
}
