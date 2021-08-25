package mark

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"sync/atomic"
)

type Mark struct {
	chapter uint64
	line    uint64
	ctx     context.Context
	listeners sync.Map

	hasChange chan bool

}

func NewMark(ctx context.Context, chapter, line uint64)*Mark {

	mark :=  &Mark{
		ctx:       ctx,
		chapter:   chapter,
		line:      line,
		listeners: sync.Map{},
	}
	return mark
}

func SyncMarks(bookMark Mark, readerMark *Mark){

}

type Sync struct {
	id string
	c  chan bool
	m  *Mark
}

func (s *Sync) Close() {
	s.m.listeners.Delete(s.id)
	close(s.c)
}

func (s *Sync) WaitForChange() bool{
	fmt.Println("WaitForChange")
	<- s.c
	fmt.Println("WaitForChange.ok")
	return true
}

func (m *Mark) NewSyncInstance() *Sync{
	uid := uuid.New().String()
	sync := &Sync{id: uid, m: m, c: make(chan bool)}
	m.listeners.Store(uid, sync)
	return sync
}


func (m *Mark) SetLine(val uint64) {
	atomic.StoreUint64(&m.line, val)
}

func (m *Mark) SetChapter(val uint64) {
	atomic.StoreUint64(&m.chapter, val)
}

func (m *Mark) syncListeners() {
	m.listeners.Range(func(key, value interface{}) bool {
		sync := value.(*Sync)
		fmt.Println("syncListeners")
		sync.c <- true
		fmt.Println("syncListeners.ok")
		return true
	})
}
func (m *Mark) AddLine() {
	fmt.Println("AddLine")
	atomic.AddUint64(&m.line, 1)
	m.syncListeners()
}

func (m *Mark) AddChapter() {
	atomic.AddUint64(&m.chapter, 1)
	m.syncListeners()
}

func (m *Mark) ResetLine() {
	atomic.AddUint64(&m.line, -m.line)
}

func (m *Mark) GetLine() uint64 {
	return atomic.LoadUint64(&m.line)
}

func (m *Mark) GetChapter() uint64 {
	return atomic.LoadUint64(&m.chapter)
}

func (m *Mark) IsBefore(otherMark Mark) bool {
	if m.chapter >= otherMark.chapter && m.line >= otherMark.line {
		return false
	}
	return true
}

func (m *Mark) IsEqual(otherMark Mark) bool {
	if m.chapter == otherMark.chapter && m.line == otherMark.line {
		return true
	}
	return false
}

