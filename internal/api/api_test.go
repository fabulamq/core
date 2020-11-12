package api

import (
	"fmt"
	"github.com/fabulamq/core/internal/infra/generator"
	"github.com/fabulamq/go-fabula/pkg/gofabula"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// go test -v ./... -p 1 -count=1
// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race

func getPath() string {
	dir, _ := os.Getwd()
	return strings.Split(dir, "/fabulamq")[0] + "/fabulamq/.data"
}

func setup() {
	os.RemoveAll(getPath() + "/")
	os.Mkdir(getPath(), os.ModePerm)
}

var consumerOffset sync.Map

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race
func TestDifferentChannelConsumers(t *testing.T) {
	_, status := Start(Config{
		Folder:           getPath(),
		OffsetPerChapter: 50,
	})
	<-status

	{
		p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})
		go func() {
			for i := 0; i < 200; i++ {
				if i == 105 {
					p.Write("topic-1", "msg_105")
				}
				_, err := p.Write("topic-1", generator.NewFooBar())
				assert.NoError(t, err)
			}
		}()
	}

	totalMsgConsumed := make(chan string)
	lastMsg := make(chan bool)

	go func() { // 160 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 3 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_1"
			return nil
		})
		lastMsg <- true
	}()
	go func() { // 50 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "2", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 0 && tail.Chapter == 1 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_2"
			return nil
		})
		lastMsg <- true
	}()

	go func() { // wait, 105 lines
		time.Sleep(1 * time.Second)
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 5 && tail.Chapter == 2 {
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_3"
			return nil
		})
		lastMsg <- true
	}()

	go func() { // only read 5 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter: 2, Line: 5}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 2 {
				assert.Equal(t, "msg_105", tail.Message)
				return fmt.Errorf("error")
			}
			totalMsgConsumed <- "ch_4"
			return nil
		})
		lastMsg <- true
	}()

	totalMap := make(map[string]int)
	totalMsg := uint64(0)
	totalOut := 0
L:
	for {
		select {
		case <-lastMsg:
			totalOut++
			if totalOut == 4 {
				break L
			}
		case id := <-totalMsgConsumed:
			totalMap[id]++
			totalMsg++
		}
	}
	assert.Equal(t, 320, int(totalMsg))

}

func TestReviewFunction(t *testing.T) {
	_, status := Start(Config{
		Folder:           getPath(),
		OffsetPerChapter: 50,
	})
	<-status

	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Host: "localhost:9998"})
	for i := 0; i < 5; i++ {
		if i == 105 {
			p.Write("topic-1", "msg_105")
		}
		_, err := p.Write("topic-1", generator.NewFooBar())
		assert.NoError(t, err)
	}

	steps := make(chan bool)

	go func() {
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Host: "localhost:9998"})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 4 {
				steps <- true
			}
			if !tail.Review && tail.Message == "here!" {
				steps <- true
			}
			return nil
		})
	}()

	<-steps
	p.Write("topic-1", "here!")
	<-steps

}

func TestMultipleReplicas(t *testing.T) {
	_, s1 := Start(Config{
		Folder:           getPath(),
		Port:             "9990",
		Hosts:            []string{"localhost:9991", "localhost:9992"},
		OffsetPerChapter: 10,
		Weight:           100,
	})
	<-s1

	_, s2 := Start(Config{
		Folder:           getPath(),
		Port:             "9991",
		Hosts:            []string{"localhost:9990", "localhost:9992"},
		OffsetPerChapter: 10,
		Weight:           90,
	})
	<-s2

	_, s3 := Start(Config{
		Folder:           getPath(),
		Port:             "9992",
		Hosts:            []string{"localhost:9990", "localhost:9991"},
		OffsetPerChapter: 10,
		Weight:           80,
	})
	<-s3
}
