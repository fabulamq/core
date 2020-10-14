package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/core/internal/infra/log"
	"github.com/google/uuid"
	"net"
)

type storyWriter struct {
	conn      net.Conn
	ctx       context.Context
	cancel    func()
	hasFinish chan bool
	*publisher
}

func newStoryWriter(ctx context.Context, conn net.Conn, c *publisher) storyWriter {
	withCancel, cancel := context.WithCancel(ctx)
	return storyWriter{conn: conn, publisher: c, ctx: withCancel, cancel: cancel, hasFinish: make(chan bool)}
}

func (sw storyWriter) Stop() {
	sw.cancel()
	<-sw.hasFinish
}
func (sw storyWriter) listen() error {
	log.Info(sw.ctx, "storyWriter.Listen")
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
			sw.pLocker.Lock()

			if len(producerMsg) == 0 {
				return fmt.Errorf("nil message")
			}

			// perform save here "topic:msg"
			chapter, line, err := sw.book.Write(producerMsg)
			if err != nil {
				return err
			}

			err = write(sw.conn, []byte(fmt.Sprintf("ok;%d;%d", chapter, line)))
			if err != nil {
				return err
			}
			sw.pLocker.Unlock()
			log.Info(sw.ctx, fmt.Sprintf("storyWriter.Listen.SendedOK: [%s]", producerMsg))
		}
	}
}

func (sw *storyWriter) store() {
	sw.pLocker.Lock()
	sw.storyWriterMap.Store(uuid.New().String(), sw)
	sw.pLocker.Unlock()
}

func (sw storyWriter) afterStop(err error) {
	sw.pLocker.Unlock()
	log.Warn(sw.ctx, "storyWriter.Listen.err", err)
}
