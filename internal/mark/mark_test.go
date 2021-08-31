package mark

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestMarkSync(t *testing.T) {

	bookMark1 := NewMark(context.Background(), 1, 4).Static()
	bookMark2 := NewMark(context.Background(), 1, 5).Static()
	reader := NewMark(context.Background(), 1, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	var routineWg sync.WaitGroup
	routineWg.Add(2)

	go func() {
		wg.Done()

		reader.SyncMarks(bookMark1)
		assert.Equal(t, uint64(4), reader.GetLine())
		routineWg.Done()
	}()

	go func() {
		wg.Done()

		reader.SyncMarks(bookMark2)

		assert.Equal(t, uint64(5), reader.GetLine())
		routineWg.Done()
	}()


	wg.Wait()
	reader.AddLine()
	time.Sleep(1 * time.Millisecond)
	reader.AddLine()
	time.Sleep(1 * time.Millisecond)
	reader.AddLine()
	time.Sleep(1 * time.Millisecond)

	routineWg.Wait()
}

func TestMarkSyncWithExitContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bookMark10 := NewMark(context.Background(), 1, 10).Static()
	bookMark20 := NewMark(context.Background(), 1, 20).Static()
	bookMark50 := NewMark(context.Background(), 1, 50).Static()
	bookMark80 := NewMark(context.Background(), 1, 80).Static()
	reader := NewMark(ctx, 1, 1)

	var wg sync.WaitGroup
	wg.Add(4)

	var routineWg sync.WaitGroup
	routineWg.Add(4)

	go func() {
		wg.Done()

		err := reader.SyncMarks(bookMark10)
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), reader.GetLine())
		routineWg.Done()
	}()
	go func() {
		wg.Done()

		err := reader.SyncMarks(bookMark20)
		assert.NoError(t, err)
		assert.Equal(t, uint64(20), reader.GetLine())
		routineWg.Done()
	}()
	go func() {
		wg.Done()

		err := reader.SyncMarks(bookMark50)
		assert.NoError(t, err)
		assert.Equal(t, uint64(50), reader.GetLine())
		routineWg.Done()
	}()
	go func() {
		wg.Done()

		err := reader.SyncMarks(bookMark80)
		assert.Error(t, err)
		routineWg.Done()
	}()


	wg.Wait()

	for {
		reader.AddLine()
		time.Sleep(1 * time.Millisecond)
		if reader.GetLine() == 70 {
			cancel()
			break
		}
	}

	routineWg.Wait()
}