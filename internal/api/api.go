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
	Master  publisherKind = "master"
	Replica publisherKind = "replica"
)

func Start(c Config) (Place, chan apiStatus) {
	place, chStatus := DeployPlace(c)
	go func() {
		chStatus <- apiStatus{Err: nil, IsReady: true}
		for {
			conn, err := place.accept()

			if err != nil {
				chStatus <- apiStatus{Err: err, IsReady: false}
			}
			place.acceptConn(conn)
		}
	}()

	return place, chStatus
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
