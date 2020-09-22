package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zeusmq/internal/services"
	"log"
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

func Start(ctx context.Context) error {
	// control first start
	mutex.Lock()
	if started {
		return nil
	}
	started = true
	mutex.Unlock()

	offset := getCurrentOffset()
	//queueFile, err := os.OpenFile("queue.txt", os.O_APPEND|os.O_WRONLY, 0644)
	queueFile, err := services.Get().OpenFile(ctx)
	if err != nil {
		return err
	}

	subscribersInfo := make([]subscriberInfo, 0)
	go func() {
		for {
			select {
			case s := <-addSubscribersChan:
				log.Println("domain.addSubscribersChan")
				// subscriber read message
				fmt.Println("addSubscribersChan")
				go func(sub subscriberInfo) {
					for {
						select {
						case msg := <-sub.outbound:
							services.Get().Write(sub.ctx, sub.Conn, []byte(msg))
						case <-sub.ctx.Done():
							return
						}
					}
				}(s)
				// subscriber write message
				go func(sub subscriberInfo) {
					for {
						line := services.Get().ReadLine(ctx, s.Conn)
						msg := NewMessage{}
						err := json.Unmarshal([]byte(line), &msg)
						if err != nil {
							return
						}
						newMessageChan <- msg
					}
				}(s)
				subscribersInfo = append(subscribersInfo, s)
			case msg := <-newMessageChan:
				mutex.Lock()
				err := services.Get().WriteFile(ctx, queueFile, queueMsg{
					Offset: offset,
					Topic:  msg.Topic,
					Msg:    msg.Message,
				}.byte())

				if err != nil {
					continue
				}
				offset++
				mutex.Unlock()
				pubMessageChan <- PubMessage{
					Topic:   msg.Topic,
					Message: msg.Message,
				}
			case msg := <-pubMessageChan:
				for _, s := range subscribersInfo {
					sub := s
					go func() {
						// errado isso!
						if <-sub.offset-1 != offset {
							return
						}
						services.Get().Write(ctx, sub.Conn, msg.write())
						sub.offset <- <-sub.offset + 1
					}()

				}
			case <-ctx.Done():
				log.Fatal("done ctx")
			}
		}
	}()
	return nil
}

func getCurrentOffset() uint64 {
	return uint64(256)
}
