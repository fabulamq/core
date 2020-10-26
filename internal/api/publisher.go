package api

import (
	"context"
	"github.com/fabulamq/core/internal/infra/log"
	"net"
	"strings"
	"sync"
)

type publisher struct {
	locker sync.Mutex
	book   *book
	// general locker

	storyReaderMap  sync.Map
	storyWriterMap  sync.Map
	storyAuditorMap sync.Map
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
			storyReader := newStoryReader(ctx, lineSpl, publisher)
			publisher.storyReaderMap.Store(storyReader.ID, storyReader)
			err := storyReader.Listen(conn)
			publisher.storyReaderMap.Delete(storyReader.ID)
			log.Warn(storyReader.ctx, "storyReader.error", err)
		case "sw":
			storyWriter := newStoryWriter(ctx, conn, publisher)
			err := storyWriter.listen()
			storyWriter.afterStop(err)
		case "r":
			// same logic as storyReader
		}
		conn.Close()
	}()
}

func (publisher *publisher) Reset() {
	publisher.storyReaderMap.Range(func(key, value interface{}) bool {
		consumer := value.(*storyReader)
		consumer.cancel()
		publisher.storyReaderMap.Delete(key)
		return true
	})
	publisher.storyWriterMap.Range(func(key, value interface{}) bool {
		prod := value.(*storyWriter)
		publisher.storyWriterMap.Delete(key)
		prod.Stop()
		return true
	})
}