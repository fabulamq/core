package api

import (
	"bufio"
	"fmt"
	"github.com/hpcloud/tail"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type book struct {
	once            sync.Once
	currChapter     int64 // each chapter is a file
	currChapterLine int64
	chapter         *os.File
	l               sync.Mutex
	//
	maxLinesPerChapter int64
	Folder             string
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
	book.currChapterLine = offset + 1
	book.chapter = file
	book.currChapter = lastChapter
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

func (b *book) Write(bs []byte) (string, error) {
	b.l.Lock()
	_, err := b.chapter.Write(append(bs, []byte("\n")...))
	if err != nil {
		return "", err
	}
	b.addOffset()
	if b.getOffset() == b.maxLinesPerChapter {
		err = b.chapter.Close()
		if err != nil {
			return "", err
		}
		b.addChapter()
		b.resetOffset()
		err = b.newChapter(b.getChapter())
		if err != nil {
			return "", err
		}
	}
	b.l.Unlock()
	return fmt.Sprintf("%d_%d", b.getOffset(),b.getOffset()), nil
}

func (b *book) addOffset() {
	atomic.AddInt64(&b.currChapterLine, 1)
	runtime.Gosched()
}


func (b *book) addChapter() {
	atomic.AddInt64(&b.currChapter, 1)
	runtime.Gosched()
}

func (b *book) getOffset() int64 {
	return atomic.LoadInt64(&b.currChapterLine)
}

func (b *book) getChapter() int64 {
	return atomic.LoadInt64(&b.currChapter)
}

func (b *book) resetOffset() {
	atomic.AddInt64(&b.currChapterLine, -b.currChapterLine)
}