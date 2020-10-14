package api

import (
	"bytes"
	"io"
	"net"
	"sync"
)

type apiStatus struct {
	err     error
	isReady bool
}

func Start(c Config) (*publisher, chan apiStatus) {
	chStatus := make(chan apiStatus)

	book, err := startBook(bookConfig{
		MaxLinerPerChapter: c.OffsetPerChapter,
		Folder:             c.Folder,
	})
	if err != nil {
		chStatus <- apiStatus{err: err, isReady: false}
	}

	publisher := &publisher{
		book:    book,
		pLocker: sync.Mutex{},
		locker:  sync.Mutex{},
	}

	<- publisher.startAuditor()

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
			publisher.acceptConn(conn)
		}
	}()

	return publisher, chStatus
}

type Config struct {
	Host             string
	Folder           string
	OffsetPerChapter int64
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
