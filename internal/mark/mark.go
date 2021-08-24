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


func (m *Mark) WaitForChange() bool{
	fmt.Println("WaitForChange")
	uid := uuid.New().String()
	chChange := make(chan bool)
	m.listeners.Store(uid, chChange)
	L: select {
	case <- chChange:
		fmt.Println("<- chChange")
		break L
	case <- m.ctx.Done():
		break L
	}
	m.listeners.Delete(uid)
	fmt.Println("break")
	return true
}

func (m *Mark) SetLine(val uint64) {
	atomic.StoreUint64(&m.line, val)
}

func (m *Mark) SetChapter(val uint64) {
	atomic.StoreUint64(&m.chapter, val)
}

func (m *Mark) syncListeners() {
	m.listeners.Range(func(key, value interface{}) bool {
		value.(chan bool) <- true
		fmt.Println("syncListeners")
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

