package api

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/zeusmq/internal/infra/log"
	"net"
	"strconv"
)

type consumer struct {
	ID          string
	Topic       string
	ctx         context.Context
	hasFinish   chan bool
	cancel      func()
	Offset      int64

	controller *Controller
}

type connection struct {
	conn net.Conn
	Avg  int64
	Ch   string
}

func NewConsumer(ctx context.Context, lineSpl []string, c *Controller) *consumer {
	ctxWirtId := context.WithValue(ctx, "id", lineSpl[1])
	withCancel, cancel := context.WithCancel(ctxWirtId)

	offset, _ := strconv.ParseInt(lineSpl[3], 10, 64)
	newConsumer := &consumer{
		ID:         lineSpl[1],
		Topic:      lineSpl[2],
		Offset:     offset,
		hasFinish:  make(chan bool),
		ctx:        withCancel,
		cancel:     cancel,
		controller: c,
	}
	return newConsumer
}

func (c *consumer) afterStop(err error) {
	c.hasFinish <- true
	log.Warn(c.ctx, "producer.Listen.error", err)
}

func (c *consumer) Stop() {
	c.cancel()
	<-c.hasFinish
}

func (c *consumer) strategy() *tail.SeekInfo {
	if c.Offset == -1 {
		return &tail.SeekInfo{
			Offset: 0,
			Whence: 2,
		}
	}
	return &tail.SeekInfo{
		Offset: c.Offset,
		Whence: 0,
	}
}

func (c *consumer) Listen(conn net.Conn) error {
	log.Info(c.ctx, "consumer.Listen")

	err := write(conn, []byte("ok"))
	if err != nil {
		return err
	}

	c.controller.pLocker.Lock()
	tail, err := c.controller.file.TailFile(c.strategy())
	c.controller.pLocker.Unlock()
	if err != nil {
		return err
	}

	for {
		select {
		case <-c.ctx.Done():
			return fmt.Errorf("done ctx")
		case line := <-tail:
			msg := []byte(fmt.Sprintf("%s", line.Text))
			log.Info(c.ctx, fmt.Sprintf("consumer.Listen.readLine: [%s]", msg))
			if err != nil {
				return err
			}

			err = write(conn, msg)
			if err != nil {
				return err
			}
			chRes := readLine(conn)
			res := <-chRes
			if res.err != nil {
				return err
			}
			if string(res.b) != "ok" {
				return fmt.Errorf("NOK")
			}

			log.Info(c.ctx, fmt.Sprintf("consumer.Listen.completed: [%s]", msg))
		}
	}
}
