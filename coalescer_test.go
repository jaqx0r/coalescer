package coalescer_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaqx0r/coalescer"
)

type result[V any] struct {
	val V
	err error
}

func TestDedup(t *testing.T) {
	c := coalescer.New[string, string]()
	var mu sync.Mutex
	var callCount int

	fn := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		return "result", nil
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := c.Do(ctx, "key", fn)
			if err != nil {
				t.Error(err)
			}
			if val != "result" {
				t.Errorf("got %q, want %q", val, "result")
			}
		}()
	}
	wg.Wait()

	if callCount != 1 {
		t.Errorf("fn called %d times, want 1", callCount)
	}
}

func TestDistinctKeys(t *testing.T) {
	c := coalescer.New[string, string]()
	var mu sync.Mutex
	calls := make(map[string]int)

	fn := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return key + ":result", nil
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for _, k := range []string{"a", "b", "c"} {
		for range 3 {
			wg.Add(1)
			k := k
			go func() {
				defer wg.Done()
				val, err := c.Do(ctx, k, fn)
				if err != nil {
					t.Error(err)
				}
				if want := k + ":result"; val != want {
					t.Errorf("got %q, want %q", val, want)
				}
			}()
		}
	}
	wg.Wait()

	for k, v := range calls {
		if v != 1 {
			t.Errorf("key %q executed %d times, want 1", k, v)
		}
	}
}

func TestSequential(t *testing.T) {
	c := coalescer.New[string, string]()
	ctx := context.Background()

	val1, err := c.Do(ctx, "key", func(ctx context.Context, key string) (string, error) {
		return "first", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val1 != "first" {
		t.Errorf("got %q, want %q", val1, "first")
	}

	val2, err := c.Do(ctx, "key", func(ctx context.Context, key string) (string, error) {
		return "second", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val2 != "second" {
		t.Errorf("got %q, want %q", val2, "second")
	}
}

func TestError(t *testing.T) {
	c := coalescer.New[string, string]()

	blocked := make(chan struct{})
	proceed := make(chan struct{})

	go func() {
		c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			close(blocked)
			<-proceed
			return "", fmt.Errorf("database timeout")
		})
	}()

	<-blocked
	resultCh := make(chan result[string], 1)
	go func() {
		val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			return "ok", nil
		})
		resultCh <- result[string]{val, err}
	}()

	time.Sleep(100 * time.Millisecond)
	close(proceed)

	r := <-resultCh
	if r.err == nil {
		t.Fatalf("got (%q, %v), want error containing 'database timeout'", r.val, r.err)
	}
	if !strings.Contains(r.err.Error(), "database timeout") {
		t.Errorf("error doesn't mention 'database timeout': %v", r.err)
	}
	if r.val != "" {
		t.Errorf("val = %q, want empty", r.val)
	}
}

func TestLeaderPanic(t *testing.T) {
	c := coalescer.New[string, string]()
	blocked := make(chan struct{})

	go func() {
		defer func() { recover() }()
		c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			close(blocked)
			time.Sleep(100 * time.Millisecond)
			panic("boom")
		})
	}()

	<-blocked
	val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "success", nil
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error doesn't mention 'boom': %v", err)
	}
	if val != "" {
		t.Errorf("val = %q, want empty", val)
	}
}

func TestTakeover(t *testing.T) {
	c := coalescer.New[string, string]()

	leaderCtx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})

	go func() {
		c.Do(leaderCtx, "key", func(ctx context.Context, key string) (string, error) {
			close(ready)
			<-ctx.Done()
			return "", ctx.Err()
		})
	}()

	<-ready
	cancel()

	val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "took over", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val != "took over" {
		t.Errorf("got %q, want %q", val, "took over")
	}
}

func TestTakeoverToError(t *testing.T) {
	c := coalescer.New[string, string]()

	leaderCtx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})

	go func() {
		c.Do(leaderCtx, "key", func(ctx context.Context, key string) (string, error) {
			close(ready)
			<-ctx.Done()
			return "", ctx.Err()
		})
	}()

	<-ready
	cancel()

	_, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "", fmt.Errorf("still failing")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "still failing") {
		t.Errorf("error doesn't mention 'still failing': %v", err)
	}
}

func TestTakeoverAllCancelled(t *testing.T) {
	c := coalescer.New[string, string]()

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())

	errCh := make(chan error, 2)

	go func() {
		_, err := c.Do(ctxA, "key", func(ctx context.Context, key string) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
		errCh <- err
	}()

	go func() {
		_, err := c.Do(ctxB, "key", func(ctx context.Context, key string) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancelA()
	cancelB()

	for range 2 {
		err := <-errCh
		if err == nil {
			t.Error("expected error")
		}
	}
}

func TestTakeoverChain(t *testing.T) {
	c := coalescer.New[string, string]()

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())

	go func() {
		c.Do(ctxA, "key", func(ctx context.Context, key string) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
	}()

	go func() {
		c.Do(ctxB, "key", func(ctx context.Context, key string) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancelA()
	time.Sleep(50 * time.Millisecond)
	cancelB()

	val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "c succeeded", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val != "c succeeded" {
		t.Errorf("got %q, want %q", val, "c succeeded")
	}
}

func TestPreCancelledCtx(t *testing.T) {
	c := coalescer.New[string, string]()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Do(ctx, "key", func(ctx context.Context, key string) (string, error) {
		return "should not run", nil
	})
	if err == nil {
		t.Fatal("expected error for pre-cancelled context")
	}
}

func TestZeroKey(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		c := coalescer.New[int, string]()
		val, err := c.Do(context.Background(), 0, func(ctx context.Context, key int) (string, error) {
			return "zero", nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if val != "zero" {
			t.Errorf("got %q, want %q", val, "zero")
		}
	})

	t.Run("string", func(t *testing.T) {
		c := coalescer.New[string, string]()
		val, err := c.Do(context.Background(), "", func(ctx context.Context, key string) (string, error) {
			return "empty", nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if val != "empty" {
			t.Errorf("got %q, want %q", val, "empty")
		}
	})
}

func TestCompareAndDeleteCleanup(t *testing.T) {
	c := coalescer.New[string, string]()
	blocked := make(chan struct{})
	proceed := make(chan struct{})

	go func() {
		defer func() { recover() }()
		c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			close(blocked)
			<-proceed
			panic("test cleanup")
		})
	}()

	<-blocked

	resultCh := make(chan result[string], 1)
	go func() {
		val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			return "fresh", nil
		})
		resultCh <- result[string]{val, err}
	}()

	time.Sleep(100 * time.Millisecond)
	close(proceed)

	r := <-resultCh
	if r.err == nil {
		t.Fatalf("got (%q, %v), want error from panic", r.val, r.err)
	}
	if !strings.Contains(r.err.Error(), "test cleanup") {
		t.Errorf("error doesn't mention 'test cleanup': %v", r.err)
	}

	val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "after cleanup", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if val != "after cleanup" {
		t.Errorf("got %q, want %q", val, "after cleanup")
	}
}

func TestSentinelError(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", coalescer.ErrCoalescer)
	if !errors.Is(wrapped, coalescer.ErrCoalescer) {
		t.Error("errors.Is should unwrap ErrCoalescer")
	}
	doublyWrapped := fmt.Errorf("top: %w", wrapped)
	if !errors.Is(doublyWrapped, coalescer.ErrCoalescer) {
		t.Error("errors.Is should unwrap nested ErrCoalescer")
	}
}

func TestForget(t *testing.T) {
	c := coalescer.New[string, string]()
	blocked := make(chan struct{})
	proceed := make(chan struct{})

	// Leader blocks until we say go.
	go func() {
		c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			close(blocked)
			<-proceed
			return "leader", nil
		})
	}()

	<-blocked

	resultCh := make(chan result[string], 1)
	go func() {
		val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			return "retry", nil
		})
		resultCh <- result[string]{val, err}
	}()

	// Let waiter attach, then forget.
	time.Sleep(20 * time.Millisecond)
	c.Forget("key")
	close(proceed)

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.val != "retry" {
			t.Errorf("got %q, want %q", r.val, "retry")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result after Forget")
	}
}

func TestForgetRetry(t *testing.T) {
	c := coalescer.New[string, string]()

	var mu sync.Mutex
	callCount := 0

	blocked := make(chan struct{})
	proceed := make(chan struct{})

	go func() {
		c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			mu.Lock()
			callCount++
			mu.Unlock()
			close(blocked)
			<-proceed
			return "first", nil
		})
	}()

	<-blocked

	resultCh := make(chan result[string], 1)
	go func() {
		val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			mu.Lock()
			callCount++
			mu.Unlock()
			return "second", nil
		})
		resultCh <- result[string]{val, err}
	}()

	// Waiter attaches, then we forget — waiter must retry → second fn call.
	time.Sleep(20 * time.Millisecond)
	c.Forget("key")
	close(proceed)

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.val != "second" {
			t.Errorf("got %q, want %q", r.val, "second")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	mu.Lock()
	count := callCount
	mu.Unlock()
	if count != 2 {
		t.Errorf("fn called %d times, want 2", count)
	}
}

func TestPanicWrapsError(t *testing.T) {
	c := coalescer.New[string, string]()
	blocked := make(chan struct{})
	leaderDone := make(chan struct{})

	// Leader goroutine — will panic
	go func() {
		defer close(leaderDone)
		defer func() { recover() }()
		c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
			close(blocked)
			time.Sleep(50 * time.Millisecond)
			panic("something went wrong")
		})
	}()

	// Wait until leader is in-flight
	<-blocked

	// Waiter receives error from done channel — no panic here
	_, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "unreachable", nil
	})
	<-leaderDone

	if err == nil {
		t.Fatal("expected error wrapping ErrCoalescer, got nil")
	}
	if !errors.Is(err, coalescer.ErrCoalescer) {
		t.Errorf("error does not wrap ErrCoalescer: %v", err)
	}
	if !strings.Contains(err.Error(), "something went wrong") {
		t.Errorf("error missing panic message: %v", err)
	}
}

func TestForgetNoOp(t *testing.T) {
	c := coalescer.New[string, string]()
	// Forget on non-existent key must not panic
	c.Forget("nonexistent")
	val, err := c.Do(context.Background(), "key", func(ctx context.Context, key string) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "ok" {
		t.Errorf("want ok, got %v", val)
	}
}
