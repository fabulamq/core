package api

import (
	"bytes"
	"context"
	"github.com/zeusmq/internal/infra/log"
	"io"
	"net"
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
				hasFinish:  make(chan bool),
				offset:     &k,
				conn:       conn,
				ctx:        withCancel,
				cancel:     cancel,
				Controller: controller,
			}
			err := consumerInfo.Listen()
			consumerInfo.remove()
			consumerInfo.cLock.Unlock()
			consumerInfo.hasFinish <- true
			log.Warn(ctx, "producer.Listen.error", err)
		case "p":
			producer := producer{conn: conn, Controller: controller, ctx: ctx, cancel: cancel, hasFinish: make(chan bool)}
			err := producer.listen()

			controller.pLocker.Unlock()
			log.Warn(ctx, "producer.Listen.err", err)
		case "r":
		}
		conn.Close()
	}()
}

func (controller *Controller) Reset() {
	controller.consumerMap.Range(func(key, value interface{}) bool {
		if key.(string)[0:1] == "c" {
			cons := value.(*consumer)
			cons.Stop()
		}
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

//func (c *consumer) canConsume() bool {
//
//}

func getMsgOffset(msg []byte) uint64 {
	bInt := msg[0:bytes.IndexByte(msg, ';')]
	r, _ := strconv.ParseUint(string(bInt), 10, 64)
	return r
}
