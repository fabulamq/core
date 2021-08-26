package api

import (
	"context"
	"fmt"
	"net"
)

type storySync struct {
	conn      net.Conn
	ctx       context.Context
	publisher *publisher
	targetID  string
}

func (ss storySync) wait() error {
	bookMark := ss.publisher.book.mark.Static()
	value, ok := ss.publisher.storyReaderMap.Load(ss.targetID)
	if !ok {
		return fmt.Errorf("no reader stored")
	}
	storyReader := value.(storyReader)
	err := storyReader.mark.SyncMarks(bookMark)
	if err != nil {
		write(ss.conn, []byte(`msg;{"sync": false}`))
		return err
	}
	return write(ss.conn, []byte(`msg;{"sync": true}`))
}

func newStorySync(ctx context.Context, targetID string, conn net.Conn, p *publisher) *storySync {
	return &storySync{
		conn:      conn,
		ctx:       ctx,
		publisher: p,
		targetID: targetID,
	}
}