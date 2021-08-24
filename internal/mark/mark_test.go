package mark

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestMarkSync(t *testing.T) {
	mark := NewMark(context.Background(), 1, 1)

	var wg sync.WaitGroup
	wg.Add(2)

	var routineWg sync.WaitGroup
	routineWg.Add(2)

	go func() {
		wg.Done()
		if mark.WaitForChange() {
			assert.Equal(t, uint64(2), mark.line)
			fmt.Println("1")
			routineWg.Done()
		}
	}()

	go func() {
		wg.Done()
		if mark.WaitForChange() {
			assert.Equal(t, uint64(2), mark.line)
			fmt.Println("2")
			routineWg.Done()
		}
	}()

	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	mark.AddLine()

	routineWg.Wait()
}
