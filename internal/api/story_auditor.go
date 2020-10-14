package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/internal/infra/log"
	"github.com/google/uuid"
	"net"
	"strconv"
)

type storyReaderStatus struct {
	ID     string
	status readerStatus
}

type storyAuditor struct {
	ID             string
	mark           mark
	chReaderStatus chan storyReaderStatus
	c              *Controller
	conn           net.Conn
	ctx            context.Context
}


func newStoryAuditor(ctx context.Context, spl []string, conn net.Conn, c *Controller) storyAuditor {
	uid := uuid.New().String()
	chapter,_ := strconv.ParseInt(spl[1], 10, 64)
	line,_ := strconv.ParseInt(spl[2], 10, 64)
	sa := storyAuditor{
		ID:         uid,
		mark:       mark{
			chapter: chapter,
			line:    line,
		},
		chReaderStatus: make(chan storyReaderStatus),
		c:              c,
		conn:           conn,
		ctx:            ctx,
	}
	c.storyAuditorMap.Store(uid, sa)
	return sa
}



func (sa storyAuditor) listen() error {
	log.Info(sa.ctx, "storyAuditor.Listen")
	write(sa.conn, []byte("ok"))
	sa.c.storyReaderMap.Range(func(key, value interface{}) bool {
		storyReader := value.(*storyReader)
		write(sa.conn ,[]byte(fmt.Sprintf("%s;%s", storyReader.ID, storyReader.storyPoint())))
		return true
	})
	for {
		select {
		case rs := <- sa.chReaderStatus:
			sa.conn.Write([]byte(fmt.Sprintf("%s;%s", rs.ID, rs.status)))
		}
	}
}

func (sa storyAuditor) close() {
	sa.c.storyAuditorMap.Delete(sa.ID)
}