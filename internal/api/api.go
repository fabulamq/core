package api

import (
	"bytes"
	"io"
)

type apiStatus struct {
	Err     error
	IsReady bool
	kind    publisherKind
}

type publisherKind string

const (
	Unique  publisherKind = "unique"
	Master  publisherKind = "master"
	Replica publisherKind = "replica"
)

func Start(c Config) (*publisher, chan apiStatus) {
	chPlace, chStatus := deployPlace(c)
	publisher := new(publisher)
	go func() {
		chStatus <- apiStatus{Err: nil, IsReady: true}
		publisher = <- chPlace
		for {
			conn, err := publisher.accept()

			if err != nil {
				chStatus <- apiStatus{Err: err, IsReady: false}
			}
			publisher.acceptConn(conn)
		}
	}()

	return publisher, chStatus
}

type Config struct {
	Port             string
	Weight           int
	Hosts            []string
	Folder           string
	OffsetPerChapter uint64
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
