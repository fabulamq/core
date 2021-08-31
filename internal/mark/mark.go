package mark

import (
	"context"
	"sync"
	"sync/atomic"
)

type Mark struct {
	chapter uint64
	line    uint64
	ctx     context.Context
	listeners sync.Map
}

type StaticMark struct {
	chapter uint64
	line    uint64
}

func NewMark(ctx context.Context, chapter, line uint64) *Mark {
	mark :=  &Mark{
		ctx:       ctx,
		chapter:   chapter,
		line:      line,
		listeners: sync.Map{},
	}
	return mark
}


func (m *Mark) SetLine(val uint64) {
	atomic.StoreUint64(&m.line, val)
}

func (m *Mark) SetChapter(val uint64) {
	atomic.StoreUint64(&m.chapter, val)
}

func (m *Mark) Static() StaticMark{
	return StaticMark{
		chapter: m.GetChapter(),
		line:    m.GetLine(),
	}
}

func (m *Mark) syncListeners() {
	m.listeners.Range(func(key, value interface{}) bool {
		value.(chan bool) <- true
		return true
	})
}
func (m *Mark) AddLine() {
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

func (m *Mark) IsBefore(otherMark StaticMark) bool {
	if m.GetChapter() >= otherMark.chapter && m.GetLine() >= otherMark.line {
		return false
	}
	return true
}

func (m *Mark) IsEqual(otherMark StaticMark) bool {
	if m.GetChapter() == otherMark.chapter && m.GetLine() == otherMark.line {
		return true
	}
	return false
}

