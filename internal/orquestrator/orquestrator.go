package orquestrator

import (
	"github.com/fabulamq/core/internal/api"
	"github.com/fabulamq/core/internal/election"
	"github.com/fabulamq/core/internal/entity"
)

type Status struct {
	Err error
}

func Start(c api.Config) (error, chan Status) {
	chStatus := make(chan Status)
	book, err := entity.StartBook(entity.BookConfig{
		MaxLinerPerChapter: c.OffsetPerChapter,
		Folder:             c.Folder,
	})
	if err != nil {
		return err, chStatus
	}

	go func() {
		for {
			var electionResult entity.PublisherKind
		L:
			for {
				select {
				case status := <-election.Start(c, book.Mark):
					if status.Err != nil {
						chStatus <- Status{Err: err}
						continue
					}
					if status.FinishElection {
						break L
					}
					electionResult = status.Kind
				}
			}
			if electionResult == entity.HeadQuarter {

			} else {

			}
		}
	}()
}
