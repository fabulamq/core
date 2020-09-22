package domain

import (
	"context"
	"encoding/json"
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
	outbound chan string
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
	return []byte("hello")
}

var addSubscribersChan chan subscriberInfo
var newMessageChan chan NewMessage
var pubMessageChan chan PubMessage

func AddSubscriber(ctx context.Context, conn net.Conn) error {
	log.Println("domain.AddSubscriber")
	line := services.Get().ReadLine(ctx, conn)
	consumerId := subscriberInfo{}
	err := json.Unmarshal([]byte(line), &consumerId)
	if err != nil {
		return err
	}
	consumerId.Conn = conn
	addSubscribersChan <- consumerId
	return nil
}
