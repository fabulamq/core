package entity

import (
	"bufio"
	"fmt"
	"github.com/hpcloud/tail"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Book struct {
	once    sync.Once
	Mark    *Mark
	chapter *os.File
	l       sync.Mutex
	//
	MaxLinesPerChapter uint64
	Folder             string
}

type BookConfig struct {
	MaxLinerPerChapter uint64
	Folder             string
}

func StartBook(c BookConfig) (*Book, error) {
	lastChapter := uint64(0)
	hasBook := false

	book := &Book{
		once:               sync.Once{},
		Mark:               &Mark{},
		MaxLinesPerChapter: c.MaxLinerPerChapter,
		Folder:             c.Folder,
		l:                  sync.Mutex{},
	}

	err := filepath.Walk(c.Folder, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		nameSpl := strings.Split(info.Name(), ".")
		if nameSpl[1] != "chapter" {
			return nil
		}
		hasBook = true
		ch, _ := strconv.Atoi(nameSpl[0])
		if uint64(ch) > lastChapter {
			lastChapter = uint64(ch)
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
	book.Mark.line = offset + 1
	book.chapter = file
	book.Mark.chapter = lastChapter
	return book, nil
}

func lineCounter(r *os.File) (uint64, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	k := 0
	for scanner.Scan() {
		k++
	}
	return uint64(k), nil
}

func (b *Book) newChapter(i uint64) error {
	file, err := os.Create(fmt.Sprintf("%s/%d.chapter", b.Folder, i))
	if err != nil {
		return err
	}
	b.chapter = file
	return nil
}

func (b *Book) Read(chapter uint64) (chan *tail.Line, error) {
	chapterName := fmt.Sprintf("%s/%d.chapter", b.Folder, chapter)

	t, err := tail.TailFile(chapterName, tail.Config{
		Poll:        true,
		Follow:      true,
	})
	if err != nil {
		return nil, err
	}
	return t.Lines, nil
}

func (b *Book) Write(bs []byte) (*Mark, error) {
	b.l.Lock()
	defer b.l.Unlock()
	_, err := b.chapter.Write(append(bs, []byte("\n")...))
	if err != nil {
		return nil, err
	}
	b.Mark.AddLine()
	if b.Mark.GetLine() == b.MaxLinesPerChapter {
		err = b.chapter.Close()
		if err != nil {
			return nil, err
		}
		b.Mark.AddChapter()
		b.Mark.ResetLine()
		err = b.newChapter(b.Mark.GetChapter())
		if err != nil {
			return nil, err
		}
	}
	return b.Mark, nil
}

func (b *Book) Close(){
	b.chapter.Close()
}