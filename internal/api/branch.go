package api

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"time"
)

type branch struct {
	ID        string
	ctx       context.Context
	hasFinish chan bool
	cancel    func()

	mark mark

	controller    *publisher
}

func newBranch(ctx context.Context, lineSpl []string, c *publisher) *branch {
	ctxWirtId := context.WithValue(ctx, "id", lineSpl[1])
	withCancel, cancel := context.WithCancel(ctxWirtId)

	chapter, _ := strconv.ParseUint(lineSpl[2], 10, 64)
	line, _ := strconv.ParseUint(lineSpl[3], 10, 64)
	newBranch := &branch{
		ID: lineSpl[1],
		mark: mark{
			chapter: chapter,
			line:    line,
		},
		hasFinish:  make(chan bool),
		ctx:        withCancel,
		cancel:     cancel,
		controller: c,
	}
	return newBranch
}

func (br *branch) afterStop(err error) {
	br.hasFinish <- true
	log.Warn(br.ctx, "storyTeller.Listen.error", err)
}

func (br *branch) Stop() {
	br.cancel()
	<-br.hasFinish
}

func (br *branch) Listen(conn net.Conn) error {
	log.Info("branch.Listen")

	err := write(conn, []byte("ok"))
	if err != nil {
		return err
	}

	for {
		tailLine, err := br.controller.book.Read(br.mark.chapter)

		if err != nil {
			return err
		}

	Chapter:
		for {
			select {
			case <-br.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <-tailLine:
				msg := []byte(fmt.Sprintf("msg;%d;%d;%s", br.mark.getChapter(), br.mark.getLine(), line.Text))
				log.Info(fmt.Sprintf("(%s) branch.Listen.readLine: [%s]", br.ID, msg))
				if err != nil {
					return err
				}

				err = write(conn, msg)
				if err != nil {
					return err
				}
				ctxTo, _ := context.WithTimeout(br.ctx, time.Second * 3)
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

					br.mark.addLine()
					if br.mark.getLine() == br.controller.book.maxLinesPerChapter {
						log.Info(fmt.Sprintf("branch.Listen.newChapter(%s)", br.ID))
						br.mark.resetLine()
						br.mark.addChapter()
						breakChapter = true
					}

					log.Info(fmt.Sprintf("branch.Listen.completed(%s): [%s]", br.ID, msg))

					if breakChapter {
						break Chapter
					}
				}
			}
		}
	}
}