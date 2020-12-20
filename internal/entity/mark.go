package entity

import "sync/atomic"

type Mark struct {
	chapter uint64
	line    uint64
}

func NewMark(c,l uint64)*Mark{
	return &Mark{
		chapter: c,
		line:    l,
	}
}

func (m *Mark) AddLine() {
	atomic.AddUint64(&m.line, 1)
}

func (m *Mark) AddChapter() {
	atomic.AddUint64(&m.chapter, 1)
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

