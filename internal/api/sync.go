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
	currentMark := ss.publisher.book.mark
	value, ok := ss.publisher.storyReaderMap.Load(ss.targetID)
	if !ok {
		return fmt.Errorf("no reader stored")
	}
	storyReader := value.(storyReader)

	if storyReader.mark.IsEqual(*currentMark){
		return write(ss.conn, []byte("msg;sync"))
	}

	for{
		select {
		case <- storyReader.hasFinish:
			return fmt.Errorf("stopped subscriber")
		}
	}
}

func newStorySync(ctx context.Context, targetID string, conn net.Conn, p *publisher) *storySync {
	return &storySync{
		conn:      conn,
		ctx:       ctx,
		publisher: p,
		targetID: targetID,
	}
}