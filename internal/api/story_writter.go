package api

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"net"
)

type storyWriter struct {
	ID        string
	conn      net.Conn
	ctx       context.Context
	cancel    func()
	hasFinish chan bool
	publisher *publisher
}

func newStoryWriter(ctx context.Context, conn net.Conn, c *publisher) *storyWriter {
	withCancel, cancel := context.WithCancel(ctx)
	return &storyWriter{
		conn: conn,
		ID: uuid.New().String(),
		publisher: c,
		ctx: withCancel,
		cancel: cancel,
		hasFinish: make(chan bool),
	}
}

func (sw storyWriter) listen() error {
	log.Info(fmt.Sprintf("(%s) storyWriter.Listen id=%s", sw.publisher.ID, sw.ID))
	write(sw.conn, []byte("ok"))
	for {
		select {
		case <-sw.ctx.Done():
			return fmt.Errorf("finish context")
		case res := <-readLine(sw.conn):
			producerMsg := res.b
			if res.err != nil {
				return res.err
			}
			sw.publisher.locker.Lock()

			if len(producerMsg) == 0 {
				sw.publisher.locker.Unlock()
				return fmt.Errorf("nil message")
			}

			// perform save here "topic:msg"
			mark, err := sw.publisher.book.Write(producerMsg)
			if err != nil {
				sw.publisher.locker.Unlock()
				return err
			}

			err = write(sw.conn, []byte(fmt.Sprintf("ok;%d;%d", mark.getChapter(), mark.getLine())))
			if err != nil {
				sw.publisher.locker.Unlock()
				return err
			}
			sw.publisher.locker.Unlock()
			log.Info(fmt.Sprintf("storyWriter.Listen.SendedOK: [%s]", producerMsg))
		}
	}
}