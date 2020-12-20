package api

import (
	"context"
	"fmt"
	"github.com/fabulamq/core/internal/entity"
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

	mark      *entity.Mark

	controller    *publisher
}

func newBranch(ctx context.Context, lineSpl []string, p *publisher) *branch {
	p.gainBranch <- true
	withCancel, cancel := context.WithCancel(ctx)

	chapter, _ := strconv.ParseUint(lineSpl[2], 10, 64)
	line, _ := strconv.ParseUint(lineSpl[3], 10, 64)
	newBranch := &branch{
		ID: lineSpl[1],
		mark: entity.NewMark(chapter, line),
		hasFinish:  make(chan bool),
		ctx:        withCancel,
		cancel:     cancel,
		controller: p,
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
		tailLine, err := br.controller.book.Read(br.mark.GetChapter())

		if err != nil {
			return err
		}

	Chapter:
		for {
			select {
			case <-br.ctx.Done():
				return fmt.Errorf("done ctx")
			case line := <-tailLine:
				msg := []byte(fmt.Sprintf("msg;%d;%d;%s", br.mark.GetChapter(), br.mark.GetLine(), line.Text))
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

					br.mark.AddLine()
					if br.mark.GetLine() == br.controller.book.MaxLinesPerChapter {
						log.Info(fmt.Sprintf("branch.Listen.newChapter(%s)", br.ID))
						br.mark.ResetLine()
						br.mark.AddChapter()
						breakChapter = true
					}

					log.Info(fmt.Sprintf("(%s) branch.Listen.completed ID=%s: [%s]", br.controller.ID,  br.ID, msg))

					if breakChapter {
						break Chapter
					}
				}
			}
		}
	}
}