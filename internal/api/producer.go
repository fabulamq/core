package api

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/zeusmq/internal/infra/log"
	"net"
)

type producer struct {
	conn      net.Conn
	ctx       context.Context
	cancel    func()
	hasFinish chan bool
	*Controller
}

func (producer producer) Stop() {
	producer.cancel()
	<-producer.hasFinish
}
func (producer producer) listen() error {
	log.Info(producer.ctx, "producer.Listen")
	write(producer.conn, []byte("ok"))
	for {
		select {
		case <-producer.ctx.Done():
			return fmt.Errorf("finish context")
		case res := <-readLine(producer.conn):
			producerMsg := res.b
			if res.err != nil {
				return res.err
			}
			producer.pLocker.Lock()
			if len(producerMsg) == 0 {
				return fmt.Errorf("nil message")
			}

			producerMsg = append([]byte(fmt.Sprintf("%d;", producer.file.GetOffset())), producerMsg...)
			log.Info(producer.ctx, fmt.Sprintf("producer.send: [%s]", producerMsg))

			// perform save here "topic:msg"
			err := producer.file.WriteFile(producer.ctx, producerMsg)
			if err != nil {
				return err
			}

			producer.file.AddOffset()

			err = write(producer.conn, []byte("ok"))
			if err != nil {
				return err
			}
			producer.pLocker.Unlock()
			log.Info(producer.ctx, fmt.Sprintf("producer.Listen.SendedOK: [%s]", producerMsg))
		}
	}
}

func (producer *producer) store() {
	producer.sLocker.Lock()
	producer.producerMap.Store(uuid.New().String(), producer)
	producer.sLocker.Unlock()
}
