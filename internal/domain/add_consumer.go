package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zeusmq/internal/services"
	"log"
	"net"
)

type AddConsumerRequest struct {
}

type subscriberInfo struct {
	ID       string
	Ch       string
	Topics   []string
	offset   chan uint64
	Conn     net.Conn
	outbound chan PubMessage
	ctx      context.Context
	cancel   func()
}

type NewMessage struct {
	Topic   string
	Message string
}

type PubMessage struct {
	Topic   string
	Message string
}

func (pb PubMessage) write() []byte {
	return []byte(fmt.Sprintf("{\"topic\":\"%s\", \"message\":\"%s\"}\n", pb.Topic, pb.Message))
}

func (c controller) InitSubscriber(ctx context.Context, conn net.Conn) error {
	log.Println("controller.InitSubscriber")
	line, err := services.Get().ReadLine(ctx, conn)
	if err != nil {
		return err
	}

	sTemp := struct {
		ID     string
		Ch     string
		Topics []string
	}{}
	err = json.Unmarshal([]byte(line), &sTemp)
	if err != nil {
		return err
	}
	newCtx, cancel := context.WithCancel(context.Background())
	c.subsInfo <- &subscriberInfo{
		ID:       sTemp.ID,
		Ch:       sTemp.Ch,
		Topics:   sTemp.Topics,
		offset:   nil,
		Conn:     conn,
		outbound: make(chan PubMessage),
		ctx:      newCtx,
		cancel:   cancel,
	}
	return nil
}
