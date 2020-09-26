package api

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/zeusmq/internal/infra/log"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type apiStatus struct {
	err     error
	isReady bool
}

func Start() (*Controller, chan apiStatus) {
	controller := &Controller{
		consumerMap: sync.Map{},
		file:        file{offset: 1},
		pLocker:     sync.Mutex{},
	}
	chStatus := make(chan apiStatus)
	listener, err := net.Listen("tcp", "localhost:9998")
	if err != nil {
		chStatus <- apiStatus{err: err, isReady: false}
	}

	go func() {
		chStatus <- apiStatus{err: nil, isReady: true}
		for {
			conn, err := listener.Accept()

			if err != nil {
				chStatus <- apiStatus{err: err, isReady: false}
			}
			controller.start(conn)
		}
	}()

	return controller, chStatus
}

type Controller struct {
	file    file
	pLocker sync.Mutex
	sLocker sync.Mutex

	consumerMap sync.Map
}

func (controller *Controller) start(conn net.Conn) {
	go func() {
		ctx := context.Background()

		line, err := readLine(conn)
		if err != nil {
			return
		}
		lineSpl := strings.Split(string(line), ";")

		ctxWithId := context.WithValue(context.Background(), "id", lineSpl[1])
		ctx = context.WithValue(ctxWithId, "ch", lineSpl[2])
		withCancel, cancel := context.WithCancel(ctx)
		switch lineSpl[0] {
		case "c":
			k := uint64(0)
			consumerInfo := &consumer{
				ID:         lineSpl[1],
				Ch:         lineSpl[2],
				Topic:      lineSpl[3],
				Strategy:   lineSpl[4],
				cLock:      &sync.Mutex{},
				offset:     &k,
				conn:       conn,
				ctx:        withCancel,
				cancel:     cancel,
				Controller: controller,
			}
			err = consumerInfo.listen()
			consumerInfo.remove()
			consumerInfo.cLock.Unlock()
			log.Warn(ctx, "producer.listen.error", err)
		case "p":
			producer := producer{conn: conn, Controller: controller, ctx: ctx}
			err = producer.listen()
			controller.pLocker.Unlock()
			log.Warn(ctx, "producer.listen.err", err)
		case "r":
		}
		conn.Close()
	}()
}

func (controller *Controller) reset() {
	controller.consumerMap.Range(func(key, value interface{}) bool {
		if key.(string)[0:1] == "c" {
			cons := value.(*consumer)
			cons.cancel()
		}
		return true
	})
}

type producer struct {
	conn net.Conn
	ctx  context.Context
	*Controller
}

func (producer producer) listen() error {
	log.Info(producer.ctx, "producer.listen")
	write(producer.conn, []byte("ok"))
	for {
		producerMsg, err := readLine(producer.conn)
		producer.pLocker.Lock()
		if err != nil {
			return err
		}
		if len(producerMsg) == 0 {
			return fmt.Errorf("nil message")
		}

		producerMsg = append([]byte(fmt.Sprintf("%d;", producer.file.GetOffset())), producerMsg...)
		log.Info(producer.ctx, fmt.Sprintf("producer.send: [%s]", producerMsg))

		// perform save here "topic:msg"
		err = producer.file.WriteFile(producer.ctx, producerMsg)
		if err != nil {
			return err
		}

		producer.file.AddOffset()

		err = write(producer.conn, []byte("ok"))
		if err != nil {
			return err
		}
		producer.pLocker.Unlock()
		log.Info(producer.ctx, fmt.Sprintf("producer.listen.SendedOK: [%s]", producerMsg))
	}
}

func readLine(conn io.Reader) ([]byte, error) {
	buf := make([]byte, 0, 128) // big buffer
	tmp := make([]byte, 1024)   // using small tmo buffer for demonstrating
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		idx := bytes.Index(tmp, []byte("\n"))
		if idx == -1 {
			buf = append(buf, tmp[:n]...)
		} else {
			buf = append(buf, tmp[:idx]...)
			break
		}
	}
	return buf, nil
}

func write(writer io.Writer, msg []byte) error {
	_, err := writer.Write(append(msg, []byte("\n")...))
	return err
}

type file struct {
	file   *os.File
	m      sync.Mutex
	offset uint64
	once   sync.Once
}

func (f *file) createFile() {
	if _, err := os.Stat("/var/tmp/queue.txt"); os.IsNotExist(err) {
		var err error
		f.file, err = os.Create("/var/tmp/queue.txt")
		if err != nil {
			log.Fatal(context.Background(), err.Error())
		}
	}
}

func (f *file) AddOffset() {
	f.offset++
}

func (f *file) GetOffset() uint64 {
	f.once.Do(func() {
		f.file.Seek(0, 2)
	})
	return f.offset
}

func (f file) TailFile() (chan *tail.Line, error) {
	t, err := tail.TailFile("/var/tmp/queue.txt", tail.Config{Follow: true})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func (f file) OpenFile() (*os.File, error) {
	file, err := os.Open("/var/tmp/queue.txt")
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *file) WriteFile(ctx context.Context, b []byte) error {
	f.createFile()
	_, err := f.file.Write(append(b, []byte("\n")...))
	if err != nil {
		return err
	}
	return nil
}

func (f *file) CleanFile() {
	f.offset = 1
	os.Remove("/var/tmp/queue.txt")
	f.createFile()
}

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
			consumerRes, err := readLine(c.conn)
			if err != nil {
				return err
			}
			if string(consumerRes) != "ok" {
				return fmt.Errorf("error NOK")
			}
			// update offset
			*c.offset = getMsgOffset(msg)
			c.cLock.Unlock()

			log.Info(c.ctx, fmt.Sprintf("consumer.listen.readLine.complete: [%s]", msg))
		}
	}
}

//func (c *consumer) canConsume() bool {
//
//}

func getMsgOffset(msg []byte) uint64 {
	bInt := msg[0:bytes.IndexByte(msg, ';')]
	r, _ := strconv.ParseUint(string(bInt), 10, 64)
	return r
}

func (c consumer) onlyIdKey() string {
	return fmt.Sprintf("s_%s", c.ID)
}

func (c consumer) idChKey() string {
	return fmt.Sprintf("c_%s_%s", c.ID, c.Ch)
}
