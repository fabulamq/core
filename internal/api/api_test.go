package api

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fabulamq/core/internal/infra/generator"
	"github.com/fabulamq/go-fabula/pkg/gofabula"
	"github.com/stretchr/testify/assert"
)

// go test -v ./... -p 1 -count=1
// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race

func getPath() string {
	dir, _ := os.Getwd()
	return strings.Split(dir, "/fabulamq")[0] + "/fabulamq/.data"
}

func getPathS1() string {
	dir, _ := os.Getwd()
	return strings.Split(dir, "/fabulamq")[0] + "/fabulamq/.data/sub1"
}

func getPathS2() string {
	dir, _ := os.Getwd()
	return strings.Split(dir, "/fabulamq")[0] + "/fabulamq/.data/sub2"
}

func getPathS3() string {
	dir, _ := os.Getwd()
	return strings.Split(dir, "/fabulamq")[0] + "/fabulamq/.data/sub3"
}

func setup() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: false,
		ForceColors: true,
	})
	os.RemoveAll(getPath() + "/")
	os.Mkdir(getPath(), os.ModePerm)
	os.Mkdir(getPath() + "/sub1", os.ModePerm)
	os.Mkdir(getPath() + "/sub2", os.ModePerm)
	os.Mkdir(getPath() + "/sub3", os.ModePerm)
}

var consumerOffset sync.Map

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race
func TestDifferentChannelConsumers(t *testing.T) {
	api := Start(Config{
		Host:             "-",
		Folder:           getPath(),
		OffsetPerChapter: 50,
	})
	<- api.Status

	{
		p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Hosts: []string{"-"}})
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
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Hosts: []string{"-"}})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 3 {
				fmt.Println("error on ID 1")
				return fmt.Errorf("error on ID 1")
			}
			totalMsgConsumed <- "ch_1"
			return nil
		})
		lastMsg <- true
	}()
	go func() { // 50 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "2", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Hosts: []string{"-"}})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 0 && tail.Chapter == 1 {
				fmt.Println("error on ID 2")
				return fmt.Errorf("error on ID 2")
			}
			totalMsgConsumed <- "ch_2"
			return nil
		})
		lastMsg <- true
	}()

	go func() { // wait, 105 lines
		time.Sleep(1 * time.Second)
		storyReader, err := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "3", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Hosts: []string{"-"}})
		assert.NoError(t, err)
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 5 && tail.Chapter == 2 {
				fmt.Println("error on ID 3")
				return fmt.Errorf("error on ID 3")
			}
			totalMsgConsumed <- "ch_3"
			return nil
		})
		lastMsg <- true
	}()

	go func() { // only read 5 lines
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "4", Mark: gofabula.Mark{Chapter: 2, Line: 5}, Hosts: []string{"-"}})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 10 && tail.Chapter == 2 {
				assert.Equal(t, "msg_105", tail.Message)
				fmt.Println("error on ID 4")
				return fmt.Errorf("error on ID 4")
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

// go test -v ./... -p 1 -count=1 -run TestDifferentChannelConsumers -failfast -race
func TestReadingFlow(t *testing.T) {
	api := Start(Config{
		Host:             "-",
		Folder:           getPath(),
		OffsetPerChapter: 10,
	})
	<- api.Status

	{
		p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Hosts: []string{"-"}})
		go func() {
			for i := 0; i < 400; i++ {
				time.Sleep(10 * time.Millisecond)
				_, err := p.Write("topic-1", generator.NewFooBar())
				assert.NoError(t, err)
			}
		}()
	}

	hasEnd := make(chan bool)

	go func() {
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Hosts: []string{"-"}})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			fmt.Println("read here 1: ", tail.Chapter, tail.Line)
			if tail.Line == 5 && tail.Chapter == 38 {
				hasEnd <- true
				return fmt.Errorf("error on ID 1")
			}
			return nil
		})
	}()

	<-hasEnd

}

func TestReviewFunction(t *testing.T) {
	api := Start(Config{
		Host:             "-",
		Folder:           getPath(),
		OffsetPerChapter: 50,
	})
	<- api.Status

	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Hosts: []string{"-"}})
	for i := 0; i < 5; i++ {
		if i == 105 {
			p.Write("topic-1", "msg_105")
		}
		_, err := p.Write("topic-1", generator.NewFooBar())
		assert.NoError(t, err)
	}

	steps := make(chan bool)

	go func() {
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Hosts: []string{"-"}})
		storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 4 {
				steps <- true
			}
			if tail.Message == "here!" {
				steps <- true
			}
			return nil
		})
	}()

	<-steps
	p.Write("topic-1", "here!")
	<-steps

}

func TestSyncFunction(t *testing.T) {
	api := Start(Config{
		Host:             "-",
		Folder:           getPath(),
		OffsetPerChapter: 50,
	})
	<-api.Status

	p, _ := gofabula.NewStoryWriter(gofabula.ConfigWriter{Hosts: []string{"-"}})
	for i := 0; i < 5; i++ {
		_, err := p.Write("topic-1", generator.NewFooBar())
		assert.NoError(t, err)
	}

	wg := make(chan bool)

	go func() {
		wg <- true
		mark, err := gofabula.Sync("1")
		assert.NoError(t, err)
		assert.Equal(t, 5, mark.Line)
		wg <- true
	}()

	<- wg

	go func() {
		storyReader, _ := gofabula.NewStoryReader(gofabula.ConfigReader{ID: "1", Mark: gofabula.Mark{Chapter: 0, Line: 0}, Hosts: []string{"-"}})
		err := storyReader.Read(func(tail gofabula.FabulaTail) error {
			if tail.Line == 5 {
				return nil
			}
			return nil
		})
		assert.NoError(t, err)
	}()

	<- wg
}

