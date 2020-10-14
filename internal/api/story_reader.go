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

	mark      mark

	controller *publisher
}

func newStoryReader(ctx context.Context, lineSpl []string, c *publisher) *storyReader {
	ctxWirtId := context.WithValue(ctx, "id", lineSpl[1])
	withCancel, cancel := context.WithCancel(ctxWirtId)

	chapter, _ := strconv.ParseInt(lineSpl[2], 10, 64)
	line, _ := strconv.ParseInt(lineSpl[3], 10, 64)
	newConsumer := &storyReader{
		ID:         lineSpl[1],
		mark: mark{
			chapter: chapter,
			line:    line,
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

		Chapter: for {
			select {
			case <-sr.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <- chapter:
				msg := []byte(fmt.Sprintf("%d;%d;%s", sr.mark.getChapter(), sr.mark.getLine(), line.Text))
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
					sr.controller.auditor <- storyReaderStatus{
						status: cantRead,
						ID:     sr.ID,
					}
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

				sr.controller.auditor <- storyReaderStatus{
					status: sr.storyPoint(sr.controller.book.mark),
					ID: sr.ID,
				}

				if breakChapter {
					break Chapter
				}
			}
		}
	}
}


type readerStatus string

const  (
	cantRead  readerStatus = "cantRead"
	almost    readerStatus = "almost"
	farAway   readerStatus = "faraway"
	readIt    readerStatus = "readIt"
	ahead     readerStatus = "ahead"
)

func (sr *storyReader) storyPoint(m mark) readerStatus {
	status := farAway
	if sr.mark.getChapter() >= m.getChapter() {
		status = almost
		if sr.mark.getLine() == m.getLine() - 1 {
			status = readIt
		}
		if sr.mark.getLine() > m.getLine() - 1 {
			status = ahead
		}
	}
	return status
}