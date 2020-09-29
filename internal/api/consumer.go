package api

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/infra/log"
	"net"
	"sync"
	"time"
)

type consumer struct {
	ID          string
	Topic       string
	ctx         context.Context
	hasFinish   chan bool
	cancel      func()
	Strategy    string
	connections sync.Map
	locker      sync.Mutex
	// to lock message for one single consumer
	offset *uint64

	controller *Controller
}

type connection struct {
	conn net.Conn
	Avg  int64
	Ch   string
}

func NewConsumer(ctx context.Context, lineSpl []string, c *Controller) *consumer {

	ctxWithId := context.WithValue(ctx, "id", lineSpl[1])
	withCancel, cancel := context.WithCancel(ctxWithId)

	k := uint64(0)
	newConsumer := &consumer{
		ID:         lineSpl[1],
		Topic:      lineSpl[3],
		Strategy:   lineSpl[4],
		hasFinish:  make(chan bool),
		offset:     &k,
		ctx:        withCancel,
		locker:     sync.Mutex{},
		cancel:     cancel,
		controller: c,
	}
	c.consumerMap.Store(lineSpl[1], newConsumer)
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

func (c *consumer) Listen() error {
	log.Info(c.ctx, "consumer.Listen")

	tail, err := c.controller.file.TailFile()
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

			c.locker.Lock()

			success := false
			errs := make([]string, 0)
			successCh := ""
			errsCh := make([]string, 0)
			t := 0
			c.connections.Range(func(key, value interface{}) bool {
				connMap := value.(*connection)

				t += 1
				start := time.Now()

				err = write(connMap.conn, msg)
				if err != nil {
					errsCh = append(errsCh, key.(string))
					errs = append(errs, fmt.Sprintf("err on consumer %s [%s]", key, err.Error()))
					return true
				}
				chRes := readLine(connMap.conn)
				res := <-chRes
				if res.err != nil {
					errsCh = append(errsCh, key.(string))
					errs = append(errs, fmt.Sprintf("err on consumer %s [%s]", key, res.err.Error()))
					return true
				}
				if string(res.b) != "ok" {
					errsCh = append(errsCh, key.(string))
					errs = append(errs, fmt.Sprintf("err NOK %s", key))
					return true
				}
				connMap.Avg = (time.Now().Sub(start).Milliseconds() + connMap.Avg) / 2
				success = true
				successCh = key.(string)
				return false
			})

			fmt.Println("AAA", errsCh)

			if success {
				*c.offset = getMsgOffset(msg)
			} else {
				errMsg := ""
				for _, err := range errs {
					errMsg += fmt.Sprintf("[%s]", err)
				}
				c.locker.Unlock()
				return fmt.Errorf("no success on msg [%s]: {%s} - %d", msg, errMsg, t)
			}
			c.locker.Unlock()
			log.Info(c.ctx, fmt.Sprintf("consumer.Listen.completed: Ch:[%s] [%s]", successCh, msg))
		}
	}
}

func (c consumer) onlyIdKey() string {
	return fmt.Sprintf("s_%s", c.ID)
}

func (c *consumer) addChannel(ch string, conn net.Conn) {
	write(conn, []byte("ok"))
	c.connections.Store(ch, &connection{
		conn: conn,
		Ch:   ch,
	})
}
