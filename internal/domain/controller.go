package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zeusmq/internal/infra/log"
	"github.com/zeusmq/internal/services"
	"os"
	"sync"
)

var mutex sync.Mutex
var started bool

type queueMsg struct {
	Offset uint64
	Topic  string
	Msg    string
}

func (q queueMsg) byte() []byte {
	k := fmt.Sprintf("%d;%s", q.Offset, q.Msg)
	return []byte(k)
}

type controller struct {
	consumerInfo chan *consumerInfo
	producerInfo chan *producerInfo
}

func Start(ctx context.Context) (*controller, error) {
	// control first start
	mutex.Lock()
	if started {
		return nil, nil
	}
	started = true
	mutex.Unlock()

	consumerInfoChanChan := make(chan *consumerInfo)
	producerInfoChanChan := make(chan *producerInfo)

	newMessageChan := make(chan NewMessage)
	pubMessageChan := make(chan PubMessage)

	offset := getCurrentOffset()
	//queueFile, err := os.OpenFile("queue.txt", os.O_APPEND|os.O_WRONLY, 0644)
	queueFile, err := services.Get().OpenFile(ctx)
	if err != nil {
		return nil, err
	}

	subscribersInfo := make([]*consumerInfo, 0)
	go func() {
		for {
			select {
			case s := <-producerInfoChanChan:
				readFromProducer(s, newMessageChan)
			case s := <-consumerInfoChanChan:
				writeToConsumer(s)
				removeSubscriber(s)
				subscribersInfo = append(subscribersInfo, s)
			case msg := <-newMessageChan:
				newMessageStep(ctx, queueFile, offset, msg, pubMessageChan)
				offset++
			case msg := <-pubMessageChan:
				pubMessageStep(msg, subscribersInfo)
			case <-ctx.Done():
				log.Fatal(ctx, "done ctx")
			}
		}
	}()
	return &controller{
		consumerInfo: consumerInfoChanChan,
		producerInfo: producerInfoChanChan,
	}, nil
}

func removeSubscriber(sub *consumerInfo) {
	go func() {
		select {
		case <-sub.ctx.Done():
			log.Warn(sub.ctx, "domain.addSubscribersChan.exit", fmt.Errorf("subscriber gonne"))
			sub.conn.Close()
			return
		}
	}()
}

func newMessageStep(ctx context.Context, queueFile *os.File, offset uint64, msg NewMessage, pubMessageChan chan PubMessage) {
	log.Info(ctx, fmt.Sprintf("newMessageStep: %s", msg.Message))
	mutex.Lock()
	err := services.Get().WriteFile(ctx, queueFile, queueMsg{
		Offset: offset,
		Topic:  msg.Topic,
		Msg:    msg.Message,
	}.byte())

	if err != nil {
		return
	}

	msg.isPersisted <- true

	mutex.Unlock()

	go func() {
		pubMessageChan <- PubMessage{
			Topic:   msg.Topic,
			Message: msg.Message,
			Offset:  offset,
		}
	}()
}

func pubMessageStep(msg PubMessage, subscribersInfo []*consumerInfo) {
	for i, _ := range subscribersInfo {
		go func(sub *consumerInfo) {
			log.Info(sub.ctx, fmt.Sprintf("pubMessageStep: %s (%d)", msg.Message, msg.Offset))
			sub.outbound <- msg
		}(subscribersInfo[i])
	}
}

func writeToConsumer(consumer *consumerInfo) {
	// consumer read message
	err := services.Get().Write(consumer.ctx, consumer.conn, []byte("ok\n"))
	if err != nil {
		log.Warn(consumer.ctx, "domain.writeToConsumer.write", err)
		consumer.cancel()
		return
	}
	go func() {
		for {
			select {
			case msg := <-consumer.outbound:
				if msg.Topic != consumer.Topic {
					log.Info(consumer.ctx, fmt.Sprintf("domain.skipTopic: %s | %s", msg.Message, msg.Topic))
					consumer.offset++
					continue
				}
				log.Info(consumer.ctx, fmt.Sprintf("domain.writeToConsumer: %s | %s", msg.Message, msg.Topic))
				if (consumer.offset - 1) != msg.Offset {
					log.Warn(consumer.ctx, "domain.writeToConsumer.offset", fmt.Errorf("offset uncase c:%d|m:%d", consumer.offset, msg.Offset))
					consumer.cancel()
					return
				}
				err := services.Get().Write(consumer.ctx, consumer.conn, msg.write())
				if err != nil {
					log.Warn(consumer.ctx, "domain.writeToConsumer.writeError", err)
					consumer.cancel()
					return
				}
				//TODO check for an ACK here

				line, err := services.Get().ReadLine(consumer.ctx, consumer.conn)
				if line != "ok" || err != nil {
					log.Warn(consumer.ctx, "domain.writeToConsumer.writeError", err)
					consumer.cancel()
					return
				}

				consumer.offset++
			}
		}
	}()
}

func readFromProducer(prod *producerInfo, newMessageChan chan NewMessage) {
	err := services.Get().Write(prod.ctx, prod.conn, []byte("ok\n"))
	if err != nil {
		prod.cancel()
		return
	}
	// read from subscriber
	go func() {
		for {
			line, err := services.Get().ReadLine(prod.ctx, prod.conn)

			log.Info(prod.ctx, fmt.Sprintf("domain.readFromProducer: %s", line))
			if err != nil {
				prod.cancel()
				return
			}

			msg := NewMessage{}
			err = json.Unmarshal([]byte(line), &msg)
			if err != nil {
				prod.cancel()
				return
			}
			msg.isPersisted = make(chan bool)

			newMessageChan <- msg

			// wait util message is persisted
			<-msg.isPersisted
			err = services.Get().Write(prod.ctx, prod.conn, []byte("ok\n"))
			if err != nil {
				prod.cancel()
				return
			}
		}
	}()
}

func getCurrentOffset() uint64 {
	return uint64(0)
}
