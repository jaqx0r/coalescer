// Package coalescer groups concurrent identical requests so only one
// executes at a time.  Duplicate requests (identified by key) wait for
// the in-flight request to complete and share its result.
//
// A call to Do executes fn only when no request with the same key is
// currently in-flight.  All concurrent callers with that key receive the
// same result (or error).  Once fn returns, the key is released and a
// subsequent call will execute fn again.
//
// If the executing caller's context is cancelled, fn is expected to
// return ctx.Err().  Concurrent callers waiting for the result will
// continue rather than inherit the cancellation — one of them becomes
// the new leader and executes fn with its own context.
//
// Context cancellation of a waiting caller is independent: cancelling
// one waiter's context never affects the executing fn or other waiters.
package coalescer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var ErrCoalescer = errors.New("coalescer")

type call[V any] struct {
	done   chan struct{}
	val    V
	err    error
	retry  bool
	closed bool
	mu     sync.Mutex
}

// closeDone closes the done channel exactly once. Must be called with mu held.
func (cl *call[V]) closeDone() {
	if !cl.closed {
		cl.closed = true
		close(cl.done)
	}
}

type Coalescer[K comparable, V any] struct {
	inflight sync.Map
}

func New[K comparable, V any]() *Coalescer[K, V] {
	return &Coalescer[K, V]{}
}

func (c *Coalescer[K, V]) Do(ctx context.Context, key K, fn func(context.Context, K) (V, error)) (V, error) {
	for {
		cl := &call[V]{done: make(chan struct{})}
		actual, loaded := c.inflight.LoadOrStore(key, cl)
		if loaded {
			trace.SpanFromContext(ctx).AddEvent("coalescer.wait",
				trace.WithAttributes(attribute.String("coalescer.key", fmt.Sprint(key))),
			)

			entry := actual.(*call[V])
			select {
			case <-entry.done:
				if entry.retry {
					continue
				}
				return entry.val, entry.err
			case <-ctx.Done():
				var zero V
				return zero, ctx.Err()
			}
		}

		var (
			val      V
			err      error
			panicVal any
			panicked bool
		)

		if err := ctx.Err(); err != nil {
			cl.mu.Lock()
			cl.err = err
			cl.closeDone()
			cl.mu.Unlock()
			c.inflight.CompareAndDelete(key, cl)
			var zero V
			return zero, err
		}

		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicVal = r
				err = fmt.Errorf("%w: panic: %v", ErrCoalescer, r)
			}
			cl.mu.Lock()
			cl.val = val
			cl.err = err
			if !panicked && ctx.Err() != nil {
				cl.retry = true
			}
			cl.closeDone()
			cl.mu.Unlock()
			c.inflight.CompareAndDelete(key, cl)
			if panicked {
				panic(panicVal)
			}
		}()

		val, err = fn(ctx, key)
		return val, err
	}
}

// Forget evicts the in-flight call for key, if any. Current waiters will
// retry with a fresh execution; the evicted leader continues but its
// result is discarded by any waiter that races past Forget.
func (c *Coalescer[K, V]) Forget(key K) {
	if v, ok := c.inflight.LoadAndDelete(key); ok {
		cl := v.(*call[V])
		cl.mu.Lock()
		cl.retry = true
		cl.closeDone()
		cl.mu.Unlock()
	}
}
