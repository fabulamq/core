package api

import "sync/atomic"

type mark struct {
	chapter uint64
	line    uint64
}

func (m *mark) addLine() {
	atomic.AddUint64(&m.line, 1)
}

func (m *mark) addChapter() {
	atomic.AddUint64(&m.chapter, 1)
}

func (m *mark) resetLine() {
	atomic.AddUint64(&m.line, -m.line)
}

func (m *mark) getLine() uint64 {
	return atomic.LoadUint64(&m.line)
}

func (m *mark) getChapter() uint64 {
	return atomic.LoadUint64(&m.chapter)
}

func (m *mark) isBefore(otherMark mark) bool {
	if m.chapter >= otherMark.chapter && m.line >= otherMark.line {
		return false
	}
	return true
}

func (m *mark) isEqual(otherMark mark) bool {
	if m.chapter == otherMark.chapter && m.line == otherMark.line {
		return true
	}
	return false
}

