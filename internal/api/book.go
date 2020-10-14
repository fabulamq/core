package api

import (
	"bufio"
	"fmt"
	"github.com/hpcloud/tail"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type book struct {
	once            sync.Once
	mark            mark
	chapter         *os.File
	l               sync.Mutex
	//
	maxLinesPerChapter int64
	Folder             string
}

type mark struct {
	chapter int64
	line    int64
}

func (m *mark) addLine()  {
	atomic.AddInt64(&m.line, 1)
}

func (m *mark) addChapter()  {
	atomic.AddInt64(&m.chapter, 1)
}

func (m *mark) resetLine()  {
	atomic.AddInt64(&m.line, -m.line)
}

func (m *mark) getLine()int64 {
	return atomic.LoadInt64(&m.line)
}

func (m *mark) getChapter()int64 {
	return atomic.LoadInt64(&m.chapter)
}

type bookConfig struct {
	MaxLinerPerChapter int64
	Folder             string
}

func startBook(c bookConfig)(*book, error){
	lastChapter := int64(0)
	hasBook := false

	book := &book{
		once:               sync.Once{},
		maxLinesPerChapter: c.MaxLinerPerChapter,
		Folder:             c.Folder,
		l:                  sync.Mutex{},
	}

	err := filepath.Walk(c.Folder, func(path string, info os.FileInfo, err error) error {
		if info.IsDir(){
			return nil
		}
		nameSpl := strings.Split(info.Name(), ".")
		if nameSpl[1] != "chapter"{
			return nil
		}
		hasBook = true
		ch, _ := strconv.Atoi(nameSpl[0])
		if int64(ch) > lastChapter {
			lastChapter = int64(ch)
		}
		return nil
	})
	if !hasBook {
		err := book.newChapter(0)
		if err != nil {
			return nil, err
		}
		return book, nil
	}
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(fmt.Sprintf("%s/%d.chapter", c.Folder, lastChapter), os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	offset, err := lineCounter(file)
	if err != nil {
		return nil, err
	}
	book.mark.line = offset + 1
	book.chapter = file
	book.mark.chapter = lastChapter
	return book, nil
}

func lineCounter(r *os.File) (int64, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	k := 0
	for scanner.Scan(){
		k++
	}
	return int64(k), nil
}

func (b *book) newChapter(i int64)error{
	file, err := os.Create(fmt.Sprintf("%s/%d.chapter", b.Folder, i))
	if err != nil {
		return err
	}
	b.chapter = file
	return nil
}


func (b *book) Read(chapter int64) (chan *tail.Line, error) {
	t, err := tail.TailFile(fmt.Sprintf("%s/%d.chapter", b.Folder, chapter), tail.Config{
		Follow:   true,
	})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func (b *book) Write(bs []byte) (int64, int64, error) {
	b.l.Lock()
	_, err := b.chapter.Write(append(bs, []byte("\n")...))
	if err != nil {
		return 0, 0, err
	}
	b.mark.addLine()
	if b.mark.getLine() == b.maxLinesPerChapter {
		err = b.chapter.Close()
		if err != nil {
			return 0, 0, err
		}
		b.mark.addChapter()
		b.mark.resetLine()
		err = b.newChapter(b.mark.getChapter())
		if err != nil {
			return 0, 0, err
		}
	}
	b.l.Unlock()
	return b.mark.getChapter(), b.mark.getLine(), nil
}
