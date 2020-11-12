package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/core/internal/infra/log"
	"net"
	"strconv"
)

type storyReader struct {
	ID        string
	ctx       context.Context
	hasFinish chan bool
	cancel    func()

	mark mark

	breakEvenMark mark
	controller    *publisher
}

func newStoryReader(ctx context.Context, lineSpl []string, c *publisher) *storyReader {
	ctxWirtId := context.WithValue(ctx, "id", lineSpl[1])
	withCancel, cancel := context.WithCancel(ctxWirtId)

	chapter, _ := strconv.ParseUint(lineSpl[2], 10, 64)
	line, _ := strconv.ParseUint(lineSpl[3], 10, 64)
	newConsumer := &storyReader{
		ID: lineSpl[1],
		mark: mark{
			chapter: chapter,
			line:    line,
		},
		breakEvenMark: mark{
			chapter: c.book.mark.chapter,
			line:    c.book.mark.line,
		},
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

	for {
		chapter, err := sr.controller.book.Read(sr.mark.chapter)

		if err != nil {
			return err
		}

	Chapter:
		for {
			select {
			case <-sr.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <-chapter:
				msg := []byte(fmt.Sprintf("%d;%d;%t;%s", sr.mark.getChapter(), sr.mark.getLine(), sr.mark.isBefore(sr.breakEvenMark), line.Text))
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

				breakChapter := false

				sr.mark.addLine()
				if sr.mark.getLine() == sr.controller.book.maxLinesPerChapter {
					sr.mark.resetLine()
					sr.mark.addChapter()
					breakChapter = true
				}

				if breakChapter {
					break Chapter
				}
			}
		}
	}
}
