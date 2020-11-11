package api

import (
	"fmt"
	"github.com/fabulamq/go-fabula/pkg/gofabula"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// go test -v ./...  -count=1 -run TestAuditorWithTwoChapters -failfast -race
func TestWritePerformance(t *testing.T) {
	//c.book.maxLinesPerChapter = uint64(100000)

	total := 0.0
	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})
	go func() {
		for  {
			_, err := p.Write("topic-1", fmt.Sprintf("%s%s", uuid.New().String(), uuid.New().String()))
			assert.NoError(t, err)
			total++
		}
	}()
	time.Sleep(10 * time.Second)
	fmt.Println("total writes/second:", total / 10)
}
