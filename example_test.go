package coalescer_test

import (
	"context"
	"fmt"

	"github.com/jaqx0r/coalescer"
)

func Example() {
	c := coalescer.New[string, string]()
	ctx := context.Background()

	val, err := c.Do(ctx, "user:42", func(ctx context.Context, key string) (string, error) {
		return "profile data", nil
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(val)

	// Output:
	// profile data
}
