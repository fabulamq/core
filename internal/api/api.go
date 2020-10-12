package api

import (
	"bytes"
	"context"
	"github.com/zeusmq/internal/infra/log"
	"io"
	"net"
	"strings"
	"sync"
)

type apiStatus struct {
	err     error
	isReady bool
}

func Start(c Config) (*Controller, chan apiStatus) {
	chStatus := make(chan apiStatus)

	book, err := startBook(bookConfig{
		MaxLinerPerChapter: c.OffsetPerChapter,
		Folder:             c.Folder,
	})
	if err != nil {
		chStatus <- apiStatus{err: err, isReady: false}
	}

	controller := &Controller{
		book: book,
		pLocker: sync.Mutex{},
		locker:  sync.Mutex{},
	}
	listener, err := net.Listen("tcp", c.Host)
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

type Config struct {
	Host             string
	Folder           string
	OffsetPerChapter int64
}

type Controller struct {
	pLocker sync.Mutex
	book    *book
	// general locker
	locker      sync.Mutex
	consumerMap sync.Map
	producerMap sync.Map
}

func (controller *Controller) start(conn net.Conn) {
	go func() {
		ctx := context.Background()

		chRes := readLine(conn)
		res := <-chRes
		if res.err != nil {
			return
		}
		lineSpl := strings.Split(string(res.b), ";")

		switch lineSpl[0] {
		case "c":
			consumer := NewConsumer(ctx, lineSpl, controller)
			controller.consumerMap.Store(consumer.ID, consumer)
			err := consumer.Listen(conn)
			controller.consumerMap.Delete(consumer.ID)
			log.Warn(consumer.ctx, "listener.error", err)
		case "p":
			producer := NewProducer(ctx, lineSpl, conn, controller)
			err := producer.listen()
			producer.afterStop(err)
		case "r":
		}
		conn.Close()
	}()
}

func (controller *Controller) Reset() {
	controller.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(*consumer)
		consumer.cancel()
		controller.consumerMap.Delete(key)
		return true
	})
	controller.producerMap.Range(func(key, value interface{}) bool {
		prod := value.(*producer)
		controller.producerMap.Delete(key)
		prod.Stop()
		return true
	})
}

type readResult struct {
	b   []byte
	err error
}

func readLine(conn io.Reader) chan readResult {
	chRes := make(chan readResult)

	go func() {
		buf := make([]byte, 0, 128) // big buffer
		tmp := make([]byte, 1024)   // using small tmo buffer for demonstrating
		for {
			n, err := conn.Read(tmp)
			if err != nil {
				if err != io.EOF {
					chRes <- readResult{b: nil, err: err}
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
		chRes <- readResult{b: buf, err: nil}
	}()

	return chRes
}

func write(writer io.Writer, msg []byte) error {
	_, err := writer.Write(append(msg, []byte("\n")...))
	return err
}
