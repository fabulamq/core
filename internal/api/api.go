package api

import (
	"bytes"
	"context"
	"github.com/fabulamq/internal/infra/log"
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
		book:    book,
		pLocker: sync.Mutex{},
		locker:  sync.Mutex{},
	}

	isReady := controller.startAuditor()
	<-isReady

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
			controller.acceptConn(conn)
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
	locker sync.Mutex

	storyReaderMap  sync.Map
	storyWriterMap  sync.Map
	storyAuditorMap sync.Map

	auditor chan storyReaderStatus
}

func (controller *Controller) acceptConn(conn net.Conn) {
	go func() {
		ctx := context.Background()

		chRes := readLine(conn)
		res := <-chRes
		if res.err != nil {
			return
		}
		lineSpl := strings.Split(string(res.b), ";")

		switch lineSpl[0] {
		case "sr":
			storyReader := newStoryReader(ctx, lineSpl, controller)
			controller.storyReaderMap.Store(storyReader.ID, storyReader)
			err := storyReader.Listen(conn)
			controller.storyReaderMap.Delete(storyReader.ID)
			log.Warn(storyReader.ctx, "storyReader.error", err)
		case "sw":
			storyWriter := newStoryWriter(ctx, conn, controller)
			err := storyWriter.listen()
			storyWriter.afterStop(err)
		case "sa":
			storyAuditor := newStoryAuditor(ctx, lineSpl, conn, controller)
			storyAuditor.listen()
			storyAuditor.close()
		case "r":
			// same logic as storyReader
		}
		conn.Close()
	}()
}

func (controller *Controller) Reset() {
	controller.storyReaderMap.Range(func(key, value interface{}) bool {
		consumer := value.(*storyReader)
		consumer.cancel()
		controller.storyReaderMap.Delete(key)
		return true
	})
	controller.storyWriterMap.Range(func(key, value interface{}) bool {
		prod := value.(*storyWriter)
		controller.storyWriterMap.Delete(key)
		prod.Stop()
		return true
	})
}

func (controller *Controller) startAuditor() chan bool {
	isReady := make(chan bool, 0)
	saChan := make(chan storyReaderStatus, 0)
	go func() {
		isReady <- true
		for {
			select {
			case sa := <-saChan:
				go func() {
					controller.storyAuditorMap.Range(func(key, value interface{}) bool {
						auditor := value.(*storyAuditor)
						auditor.chReaderStatus <- sa
						return true
					})
				}()
			}
		}
	}()
	controller.auditor = saChan
	return isReady
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
