package mark

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestMarkSync(t *testing.T) {
	mark := NewMark(context.Background(), 1, 1)
	sync1 := mark.NewSyncInstance()
	sync2 := mark.NewSyncInstance()

	var wg sync.WaitGroup
	wg.Add(2)

	var routineWg sync.WaitGroup
	routineWg.Add(2)

	go func() {
		wg.Done()
		if sync1.WaitForChange() {
			assert.Equal(t, uint64(2), mark.line)
			fmt.Println("1")
			routineWg.Done()
		}
	}()

	go func() {
		wg.Done()
		if sync2.WaitForChange() {
			assert.Equal(t, uint64(2), mark.line)
			fmt.Println("2")
			routineWg.Done()
		}
	}()

	wg.Wait()
	//time.Sleep(10 * time.Millisecond)
	mark.AddLine()

	routineWg.Wait()
}
