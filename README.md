# coalescer

A library to coalesce identical concurrent requests in Go, so only one
executes at a time.

## API

`coalescer.New[K comparable, V any]() *Coalescer[K, V]`

Create a coalescer parameterised by key type and result type.

`c.Do(ctx context.Context, key K, fn func(context.Context, K) (V, error)) (V, error)`

Call `fn(key)` as the first caller for `key`. Concurrent callers with the
same key block and receive the same result (or error). If the leader's
context is cancelled, one waiter takes over and retries `fn` with its own
context.

## Behaviour

- **Deduplication**: only one caller executes `fn` per key at a time.
- **Takeover**: leader ctx cancelled → waiters retry rather than inherit
  the cancellation.
- **Independent cancellation**: cancelling one waiter's context never affects
  the leader or other waiters.
- **Panic isolation**: a panicking `fn` propagates to the leader's caller;
  waiters receive the error wrapped by `coalescer.ErrCoalescer`.
- **OpenTelemetry**: the wait path adds a `coalescer.wait` event to the
  caller's existing span. No-op when OTel is unconfigured.

## Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/jaqx0r/coalescer"
)

func main() {
    c := coalescer.New[string, string]()
    ctx := context.Background()

    val, err := c.Do(ctx, "user:42", func(ctx context.Context, key string) (string, error) {
        return "expensive computation", nil
    })
    if err != nil { panic(err) }
    fmt.Println(val)
}
```

## Dependencies

- Go 1.24+
- `go.opentelemetry.io/otel` (API only; noop when unconfigured)
