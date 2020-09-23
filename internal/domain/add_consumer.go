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

type consumerInfo struct {
	ID       string
	Ch       string
	Topics   []string
	offset   uint64
	conn     net.Conn
	outbound chan PubMessage
	ctx      context.Context
	cancel   func()
}

type producerInfo struct {
	conn   net.Conn
	ctx    context.Context
	cancel func()
}

type NewMessage struct {
	Topic       string
	Message     string
	isPersisted chan bool
}

type PubMessage struct {
	Topic   string
	Message string
	Offset  uint64
}

func (pb PubMessage) write() []byte {
	return []byte(fmt.Sprintf("{\"topic\":\"%s\", \"offset\":%d,\"message\":\"%s\"}\n", pb.Topic, pb.Offset, pb.Message))
}

func (c controller) InitSubscriber(ctx context.Context, conn net.Conn) error {
	line, err := services.Get().ReadLine(ctx, conn)
	if err != nil {
		return err
	}
	log.Println("controller.InitSubscriber: ", line)

	sTemp := struct {
		ID     string
		Ch     string
		Kind   string
		Topics []string
	}{}
	err = json.Unmarshal([]byte(line), &sTemp)
	if err != nil {
		return err
	}

	newCtx, cancel := context.WithCancel(context.Background())
	if sTemp.Kind == "c" {
		c.consumerInfo <- &consumerInfo{
			ID:     sTemp.ID,
			Ch:     sTemp.Ch,
			Topics: sTemp.Topics,
			offset: uint64(1),
			conn:   conn,

			outbound: make(chan PubMessage),
			ctx:      newCtx,
			cancel:   cancel,
		}
	} else {
		c.producerInfo <- &producerInfo{
			conn:   conn,
			ctx:    newCtx,
			cancel: cancel,
		}
	}

	return nil
}
