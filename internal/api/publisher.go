package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/core/pkg/cnet"
	log "github.com/sirupsen/logrus"
	"net"
	"strings"
	"sync"
)

type publisher struct {
	ID            string
	Status        chan apiStatus
	HqUrl         string

	locker   sync.Mutex
	book     *book
	weight   int
	listener cnet.Listener

	storyReaderMap  sync.Map
	storyWriterMap  sync.Map
	branchMap       sync.Map
	Hosts           []string
}

func deployPublisher(c Config) *publisher {
	chStatus := make(chan apiStatus)

	book, err := startBook(bookConfig{
		MaxLinerPerChapter: c.OffsetPerChapter,
		Folder:             c.Folder,
	})
	if err != nil {
		chStatus <- apiStatus{Err: err, IsReady: false}
	}


	listener, err := cnet.Listen(cnet.ListenerConfig{Host: c.Host})
	if err != nil {
		chStatus <- apiStatus{Err: err, IsReady: false}
	}


	return &publisher{
		ID:              c.ID,
		Status:          chStatus,
		book:            book,
		listener:        listener,
		storyReaderMap:  sync.Map{},
		storyWriterMap:  sync.Map{},
	}
}


func (publisher *publisher) acceptConn(conn net.Conn) {
	go func() {
		ctx := context.Background()

		chRes := readLine(conn)
		res := <- chRes
		if res.err != nil {
			return
		}
		lineSpl := strings.Split(string(res.b), ";")

		switch lineSpl[0] {
		case "sr":
			if !publisher.acceptStoryReader() {
				break
			}
			storyReader := newStoryReader(ctx, lineSpl, publisher)
			publisher.storyReaderMap.Store(storyReader.ID, storyReader)
			err := storyReader.Listen(conn)
			publisher.storyReaderMap.Delete(storyReader.ID)
			log.Warn("storyReader.error", err)
		case "sw":
			if !publisher.acceptStoryWriter() {
				break
			}
			storyWriter := newStoryWriter(ctx, conn, publisher)
			publisher.storyWriterMap.Store(storyWriter.ID, storyWriter)
			err := storyWriter.listen()
			publisher.storyWriterMap.Delete(storyWriter.ID)
			log.Warn(fmt.Sprintf("(%s) storyWriter.error: %s", publisher.ID ,err.Error()))
		}
		conn.Close()
	}()
}


func (publisher *publisher) acceptStoryReader()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	return true
}


func (publisher *publisher) acceptStoryWriter()bool{
	publisher.locker.Lock()
	defer publisher.locker.Unlock()
	return true
}

