package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/core/internal/mark"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"time"
)

type storyReader struct {
	ID        string
	ctx       context.Context
	hasFinish chan bool
	cancel    func()

	mark *mark.Mark

	controller    *publisher
}

func newStoryReader(ctx context.Context, lineSpl []string, c *publisher) *storyReader {
	log.Info(fmt.Sprintf("(%s) newStoryReader with ID=%s", c.ID, lineSpl[1]))
	withCancel, cancel := context.WithCancel(ctx)

	chapter, _ := strconv.ParseUint(lineSpl[2], 10, 64)
	line, _ := strconv.ParseUint(lineSpl[3], 10, 64)
	newConsumer := &storyReader{
		ID:         lineSpl[1],
		mark:       mark.NewMark(withCancel, chapter, line),
		hasFinish:  make(chan bool),
		ctx:        withCancel,
		cancel:     cancel,
		controller: c,
	}
	return newConsumer
}

func (sr *storyReader) afterStop(err error) {
	sr.hasFinish <- true
	log.Warn("storyTeller.Listen.error", err)
}

func (sr *storyReader) Stop() {
	sr.cancel()
	<-sr.hasFinish
}

func (sr *storyReader) Listen(conn net.Conn) error {
	log.Info("storyReader.Listen")

	err := write(conn, []byte("ok"))
	if err != nil {
		return err
	}

	for {
		tailLine, err := sr.controller.book.Read(sr.mark.GetChapter())
		if err != nil {
			return err
		}

	Chapter:
		for {
			select {
			case <-sr.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <-tailLine:
				msg := []byte(fmt.Sprintf("msg;%d;%d;%s", sr.mark.GetChapter(), sr.mark.GetLine(), line.Text))
				log.Info(fmt.Sprintf("storyReader.Listen.readLine(%s): [%s]",sr.ID, msg))
				if err != nil {
					return err
				}

				err = write(conn, msg)
				if err != nil {
					return err
				}

				ctxTo, _ := context.WithTimeout(sr.ctx, time.Second * 3)
				select {
				case <- ctxTo.Done():
					return fmt.Errorf("timeout ctx")
				case res := <- readLine(conn):
					if res.err != nil {
						return err
					}
					if string(res.b) != "ok" {
						return fmt.Errorf("NOK")
					}

					breakChapter := false

					sr.mark.AddLine()
					if sr.mark.GetLine() == sr.controller.book.maxLinesPerChapter {
						log.Info(fmt.Sprintf("storyReader.Listen.newChapter(%s)", sr.ID))
						sr.mark.ResetLine()
						sr.mark.AddChapter()
						breakChapter = true
					}

					log.Info(fmt.Sprintf("storyReader.Listen.completed(%s): [%s]", sr.ID, msg))

					if breakChapter {
						break Chapter
					}
				}
			}
		}
	}
}
