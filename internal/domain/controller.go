package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zeusmq/internal/services"
	"log"
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
	subsInfo chan *subscriberInfo
}

func Start(ctx context.Context) (*controller, error) {
	// control first start
	mutex.Lock()
	if started {
		return nil, nil
	}
	started = true
	mutex.Unlock()

	addSubscribersChan := make(chan *subscriberInfo)
	newMessageChan := make(chan NewMessage)
	pubMessageChan := make(chan PubMessage)

	offset := getCurrentOffset()
	//queueFile, err := os.OpenFile("queue.txt", os.O_APPEND|os.O_WRONLY, 0644)
	queueFile, err := services.Get().OpenFile(ctx)
	if err != nil {
		return nil, err
	}

	subscribersInfo := make([]*subscriberInfo, 0)
	go func() {
		for {
			select {
			case s := <-addSubscribersChan:
				writeToSubscriber(s)
				readFromSubscriber(s, newMessageChan)
				err = services.Get().Write(s.ctx, s.Conn, []byte("ok\n"))
				if err != nil {
					s.cancel()
					return
				}
				subscribersInfo = append(subscribersInfo, s)
			case msg := <-newMessageChan:
				newMessageStep(ctx, queueFile, offset, msg, pubMessageChan)
			case msg := <-pubMessageChan:
				pubMessageStep(msg, subscribersInfo)
			case <-ctx.Done():
				log.Fatal("done ctx")
			}
		}
	}()
	return &controller{
		subsInfo: addSubscribersChan,
	}, nil
}

func newMessageStep(ctx context.Context, queueFile *os.File, offset uint64, msg NewMessage, pubMessageChan chan PubMessage) {
	log.Println("new message received: ", msg)
	mutex.Lock()
	err := services.Get().WriteFile(ctx, queueFile, queueMsg{
		Offset: offset,
		Topic:  msg.Topic,
		Msg:    msg.Message,
	}.byte())

	if err != nil {
		return
	}
	offset++
	mutex.Unlock()

	go func() {
		pubMessageChan <- PubMessage{
			Topic:   msg.Topic,
			Message: msg.Message,
		}
	}()
}

func pubMessageStep(msg PubMessage, subscribersInfo []*subscriberInfo) {
	log.Println("domain.pubMessageStep: ", msg)
	for i, _ := range subscribersInfo {
		go func(sub *subscriberInfo) {
			log.Println("send to subscriber: ", sub.ID, sub.Ch)
			sub.outbound <- msg
		}(subscribersInfo[i])
	}
}

func sendOkToSubscriber(sub *subscriberInfo) {

}
func writeToSubscriber(sub *subscriberInfo) {
	log.Println("domain.writeToSubscriber: ", sub.ID, sub.Ch)
	// subscriber read message
	go func() {
		for {
			select {
			case msg := <-sub.outbound:
				log.Println("write to subscriber: ", sub.ID, sub.Ch, msg)
				err := services.Get().Write(sub.ctx, sub.Conn, msg.write())
				if err != nil {
					return
				}
			case <-sub.ctx.Done():
				log.Println("domain.addSubscribersChan.exit: ", sub.ID)
				return
			}
		}
	}()
}

func readFromSubscriber(sub *subscriberInfo, newMessageChan chan NewMessage) {
	// read from subscriber
	go func() {
		for {
			line, err := services.Get().ReadLine(sub.ctx, sub.Conn)
			log.Println("domain.readFromSubscriber: ", sub.ID, sub.Ch)
			if err != nil {
				sub.cancel()
				return
			}
			log.Println("read subscriber message: ", line)
			msg := NewMessage{}
			err = json.Unmarshal([]byte(line), &msg)
			if err != nil {
				sub.cancel()
				return
			}
			newMessageChan <- msg
		}
	}()
}

func getCurrentOffset() uint64 {
	return uint64(256)
}
