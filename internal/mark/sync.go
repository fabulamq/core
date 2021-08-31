package mark

import (
	"fmt"
	"github.com/google/uuid"
	"time"
)


func (m *Mark) SyncMarks(bookMark StaticMark) error{
	if !m.IsBefore(bookMark) {
		return nil
	}

	hasChange := make(chan bool, 1)
	uid := uuid.New().String()
	m.listeners.Store(uid, hasChange)
	defer func() {
		m.listeners.Delete(uid)
		if (len(hasChange)) == 1 {
			<- hasChange
		}
	}()

	for {
		select {
		case <- time.After(100 * time.Millisecond):
			if !m.IsBefore(bookMark) {
				return nil
			}
		case <- hasChange:
			if !m.IsBefore(bookMark) {
				return nil
			}
		case <- m.ctx.Done():
			return fmt.Errorf("subscriber exit")
		}
	}
}

