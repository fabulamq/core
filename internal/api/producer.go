package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/internal/infra/log"
	"github.com/google/uuid"
	"net"
)

type producer struct {
	conn      net.Conn
	ctx       context.Context
	cancel    func()
	hasFinish chan bool
	*Controller
}

func NewProducer(ctx context.Context, lineSpl []string, conn net.Conn, c *Controller) producer {
	withCancel, cancel := context.WithCancel(ctx)
	return producer{conn: conn, Controller: c, ctx: withCancel, cancel: cancel, hasFinish: make(chan bool)}
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

			// perform save here "topic:msg"
			chLine, err := producer.book.Write(producerMsg)
			if err != nil {
				return err
			}

			err = write(producer.conn, []byte(fmt.Sprintf("ok;%s", chLine)))
			if err != nil {
				return err
			}
			producer.pLocker.Unlock()
			log.Info(producer.ctx, fmt.Sprintf("producer.Listen.SendedOK: [%s]", producerMsg))
		}
	}
}

func (producer *producer) store() {
	producer.pLocker.Lock()
	producer.producerMap.Store(uuid.New().String(), producer)
	producer.pLocker.Unlock()
}

func (producer producer) afterStop(err error) {
	producer.pLocker.Unlock()
	log.Warn(producer.ctx, "producer.Listen.err", err)
}
