package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/internal/infra/log"
	"net"
	"strconv"
)

type storyReader struct {
	ID        string
	ctx       context.Context
	hasFinish chan bool
	cancel    func()

	Line      int64
	Chapter   int64

	controller *Controller
}

func newStoryReader(ctx context.Context, lineSpl []string, c *Controller) *storyReader {
	ctxWirtId := context.WithValue(ctx, "id", lineSpl[1])
	withCancel, cancel := context.WithCancel(ctxWirtId)

	chapter, _ := strconv.ParseInt(lineSpl[2], 10, 64)
	line, _ := strconv.ParseInt(lineSpl[3], 10, 64)
	newConsumer := &storyReader{
		ID:         lineSpl[1],
		Line:       line,
		Chapter:    chapter,
		hasFinish:  make(chan bool),
		ctx:        withCancel,
		cancel:     cancel,
		controller: c,
	}
	return newConsumer
}

func (sr *storyReader) afterStop(err error) {
	sr.hasFinish <- true
	log.Warn(sr.ctx, "storyTeller.Listen.error", err)
}

func (sr *storyReader) Stop() {
	sr.cancel()
	<-sr.hasFinish
}

func (sr *storyReader) Listen(conn net.Conn) error {
	log.Info(sr.ctx, "storyReader.Listen")

	err := write(conn, []byte("ok"))
	if err != nil {
		return err
	}

	currLine := sr.Line


	for {
		chapter, err := sr.controller.book.Read(sr.Chapter)

		if err != nil {
			return err
		}

		Chapter: for {
			select {
			case <-sr.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <- chapter:
				msg := []byte(fmt.Sprintf("%d;%d;%s", sr.Chapter, currLine, line.Text))
				log.Info(sr.ctx, fmt.Sprintf("storyReader.Listen.readLine: [%s]", msg))
				if err != nil {
					return err
				}

				err = write(conn, msg)
				if err != nil {
					return err
				}
				chRes := readLine(conn)
				res := <-chRes
				if res.err != nil {
					return err
				}
				if string(res.b) != "ok" {
					return fmt.Errorf("NOK")
				}

				log.Info(sr.ctx, fmt.Sprintf("storyReader.Listen.completed: [%s]", msg))

				currLine++
				if currLine == sr.controller.book.maxLinesPerChapter {
					currLine = 0
					sr.Chapter++
					break Chapter
				}
			}
		}
	}

}
