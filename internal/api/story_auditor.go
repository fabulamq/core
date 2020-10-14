package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/core/internal/infra/log"
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
	mark           *mark
	chReaderStatus chan storyReaderStatus
	c              *publisher
	conn           net.Conn
	ctx            context.Context
}


func newStoryAuditor(ctx context.Context, spl []string, conn net.Conn, c *publisher) *storyAuditor {
	uid := uuid.New().String()
	chapter,_ := strconv.ParseUint(spl[1], 10, 64)
	line,_ := strconv.ParseUint(spl[2], 10, 64)
	sa := &storyAuditor{
		ID:         uid,
		mark:       &mark{
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
	err := write(sa.conn, []byte("ok"))
	if err != nil {
		return err
	}

	mapReaderStatus := make(map[string]readerStatus)
	sa.c.storyReaderMap.Range(func(key, value interface{}) bool {
		storyReader := value.(*storyReader)
		storyPoint := storyReader.storyPoint(sa.mark)
		mapReaderStatus[storyReader.ID] = storyPoint
		log.Info(sa.ctx, fmt.Sprintf("storyAuditor.Range: %v",storyReaderStatus{
			ID:     storyReader.ID,
			status: storyPoint,
		}))
		write(sa.conn ,[]byte(fmt.Sprintf("%s;%s", storyReader.ID, storyPoint)))
		return true
	})
	for {
		select {
		case rs := <- sa.chReaderStatus:
			if status, ok := mapReaderStatus[rs.ID]; ok {
				if status == rs.status {
					continue
				}
			}
			mapReaderStatus[rs.ID] = rs.status
			log.Info(sa.ctx, fmt.Sprintf("storyAuditor.chReaderStatus: %v", rs))
			write(sa.conn, []byte(fmt.Sprintf("%s;%s", rs.ID, rs.status)))
		}
	}
}

func (sa storyAuditor) close() {
	sa.c.storyAuditorMap.Delete(sa.ID)
}