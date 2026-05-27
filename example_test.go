package coalescer_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jaqx0r/coalescer"
)

func Example() {
	c := coalescer.New[string, string]()
	ctx := context.Background()
	ready := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Do(ctx, "expensive", func(ctx context.Context, key string) (string, error) {
			close(ready)
			time.Sleep(10 * time.Millisecond)
			return "computed result", nil
		})
	}()

	<-ready

	val2, _ := c.Do(ctx, "expensive", func(ctx context.Context, key string) (string, error) {
		return "this won't run", nil
	})
	fmt.Println("caller 2:", val2)

	wg.Wait()

	// Output:
	// caller 2: computed result
}
