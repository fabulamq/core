package api

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/infra/log"
	"net"
	"strconv"
)

type consumer struct {
	ID        string
	ctx       context.Context
	hasFinish chan bool
	cancel    func()

	Line      int64
	Chapter   int64

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

	chapter, _ := strconv.ParseInt(lineSpl[2], 10, 64)
	line, _ := strconv.ParseInt(lineSpl[3], 10, 64)
	newConsumer := &consumer{
		ID:         lineSpl[1],
		Line:       line,
		Chapter:    chapter,
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

func (c *consumer) Listen(conn net.Conn) error {
	log.Info(c.ctx, "consumer.Listen")

	err := write(conn, []byte("ok"))
	if err != nil {
		return err
	}

	currLine := c.Line


	for {
		chapter, err := c.controller.book.Read(c.Chapter)

		if err != nil {
			return err
		}

		Chapter: for {
			select {
			case <-c.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <- chapter:
				msg := []byte(fmt.Sprintf("%d;%d;%s", c.Chapter, currLine, line.Text))
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

				currLine++
				if currLine == c.controller.book.maxLinesPerChapter {
					currLine = 0
					c.Chapter++
					break Chapter
				}
			}
		}
	}

}
