package api

import (
	"context"
	"fmt"
	"github.com/zeusmq/internal/infra/log"
	"net"
	"sync"
)

type consumer struct {
	ID       string
	Ch       string
	Topic    string
	ctx      context.Context
	cancel   func()
	Strategy string
	conn     net.Conn

	// to lock message for one single consumer
	cLock  *sync.Mutex
	offset *uint64

	*Controller
}

func (c *consumer) store() {
	c.sLocker.Lock()

	// get parameters
	{
		if idMap, ok := c.consumerMap.Load(c.onlyIdKey()); ok {
			for _, consumer := range idMap.(map[string]*consumer) {
				c.cLock = consumer.cLock
				c.offset = consumer.offset
			}
		}
	}

	// store composed
	{
		_, ok := c.consumerMap.Load(c.idChKey())
		if !ok {
			c.consumerMap.Store(c.idChKey(), c)
		}
	}
	// store single
	{
		idMap, ok := c.consumerMap.Load(c.onlyIdKey())
		if ok {
			idMap.(map[string]*consumer)[c.Ch] = c
		} else {
			c.consumerMap.Store(c.onlyIdKey(), map[string]*consumer{c.Ch: c})
		}
	}
	c.sLocker.Unlock()
}

func (c *consumer) remove() {
	c.consumerMap.Delete(c.idChKey())
	if idMap, ok := c.consumerMap.Load(c.onlyIdKey()); ok {
		delete(idMap.(map[string]*consumer), c.Ch)
	}
}
func (c *consumer) listen() error {
	log.Info(c.ctx, "consumer.listen")
	write(c.conn, []byte("ok"))

	c.store()

	tail, err := c.file.TailFile()
	if err != nil {
		return err
	}

	for {
		select {
		case <-c.ctx.Done():
			c.cLock.Lock()
			return fmt.Errorf("done ctx")
		case line := <-tail:
			msg := []byte(fmt.Sprintf("%s", line.Text))
			log.Info(c.ctx, fmt.Sprintf("consumer.listen.readLine: [%s]", msg))
			if err != nil {
				return err
			}
			c.cLock.Lock()

			// check if can be consumed
			msgOffset := getMsgOffset(msg)
			if *c.offset+1 != msgOffset {
				log.Info(c.ctx, fmt.Sprintf("consumer.listen.skip: [%s], current offset: %d", msg, *c.offset))
				c.cLock.Unlock()
				continue
			}

			err = write(c.conn, msg)
			if err != nil {
				return err
			}
			chRes := readLine(c.conn)
			res := <-chRes
			if res.err != nil {
				return err
			}
			if string(res.b) != "ok" {
				return fmt.Errorf("error NOK")
			}
			// update offset
			*c.offset = getMsgOffset(msg)
			c.cLock.Unlock()

			log.Info(c.ctx, fmt.Sprintf("consumer.listen.completed: [%s]", msg))
		}
	}
}

func (c consumer) onlyIdKey() string {
	return fmt.Sprintf("s_%s", c.ID)
}

func (c consumer) idChKey() string {
	return fmt.Sprintf("c_%s_%s", c.ID, c.Ch)
}
