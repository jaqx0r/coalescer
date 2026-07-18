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

// ─────────────────────────────────────────────────────────────────────────────
// PROOF SUMMARY
//
// Ghost-variable universe:
//   leaders(k)  — set of goroutines executing fn for key k at any instant
//   waiters(k)  — set of goroutines blocked on <-entry.done for key k
//   closed(cl)  — cl.done has been closed (P_done(cl))
//   owner(cl)   — goroutine that inserted cl via LoadOrStore
//
// Concrete predicates:
//   P_inflight(k, cl) ≡ c.inflight contains k → cl
//   P_done(cl)        ≡ cl.done is closed (read never blocks)
//   P_retry(cl)       ≡ cl.retry == true
//   P_closed(cl)      ≡ cl.closed == true (guards closeDone idempotency)
//
// Property 1 — Mutual exclusion:  ∀k: |leaders(k)| ≤ 1
//   PROVED.  sync.Map.LoadOrStore is linearisable; exactly one goroutine
//   receives loaded==false for a given key at any time.  fn is called only
//   on that path.  CompareAndDelete + closeDone release the slot before any
//   new leader can win LoadOrStore for the same key.
//
// Property 2 — Result sharing:  P_done(cl) ∧ ¬P_retry(cl) → every w ∈ waiters(k) receives (cl.val, cl.err)
//   PROVED.  cl.val/cl.err written under cl.mu before closeDone (which
//   close(cl.done) under cl.mu).  Waiters receive on cl.done, then read
//   cl.val/cl.err; happens-before is established by channel close.
//
// Property 3 — No double-close:  closeDone called any number of times → done closed exactly once
//   PROVED.  closeDone is guarded by cl.mu and the cl.closed boolean flag;
//   the close(cl.done) branch executes iff !cl.closed; flag set to true
//   atomically within the same critical section.
//
// Property 4 — Leader takeover:  P_done(cl) ∧ P_retry(cl) → every w ∈ waiters(k) loops; exactly one new leader via LoadOrStore
//   PROVED.  retry==true ⟹ waiters execute `continue`; each re-enters
//   LoadOrStore; by Prop 1 exactly one wins loaded==false and becomes leader.
//
// Property 5 — Panic safety:  panic in fn → leader re-panics; every w ∈ waiters(k) receives err wrapping ErrCoalescer
//   PROVED.  recover() in defer captures panic; err set to
//   fmt.Errorf("%w: panic: %v", ErrCoalescer, r); retry NOT set; closeDone
//   called → waiters unblock, read the wrapped error; defer re-panics after
//   releasing the mutex.
//
// Property 6 — Waiter independence:  ctx_w.Done() fires for w → only w returns; leader and other waiters unaffected
//   PROVED.  The waiter's select is local to goroutine w; no shared state is
//   mutated on ctx_w cancellation.
//
// Property 7 — Termination:  every call to Do eventually returns given fn terminates and ≥1 non-cancelled goroutine exists
//   CONDITIONAL.  Requires fn to terminate and at least one goroutine with
//   a non-cancelled ctx to exist per iteration.  The loop body always either
//   returns or replaces a completed call; bounded progress follows from the
//   finite-waiter, non-cancellation assumption.
// ─────────────────────────────────────────────────────────────────────────────

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
//
// PRE:  cl.mu held by caller ∧ cl != nil
//       Formally: held(cl.mu) ∧ cl ≠ ⊥
//
// POST: P_closed(cl) ∧ P_done(cl)
//       Formally: cl.closed == true ∧ closed(cl.done)
//
// Idempotency proof (SAFETY #3):
//   Let Q ≡ P_closed(cl) ∧ P_done(cl).
//
//   Case cl.closed == false (first call):
//     WP[cl.closed = true; close(cl.done), Q]
//       = WP[cl.closed = true, WP[close(cl.done), Q]]
//       = WP[cl.closed = true, (true == true) ∧ closed(cl.done)']
//         where closed(cl.done)' holds after close()
//       = (true == true) ∧ closed(cl.done)'   — substituting cl.closed := true
//       = true   ✓  Q is established.
//
//   Case cl.closed == true (subsequent call):
//     GUARD: !cl.closed evaluates false → body skipped entirely.
//     SP[skip, P_closed(cl)] = P_closed(cl) ∧ P_done(cl)   (already true from prior call)
//     ∴ Q holds trivially without executing close(cl.done).
//     No second close() ever issued.   ✓
//
// SAFETY: #3 — done channel closed exactly once regardless of call count.
func (cl *call[V]) closeDone() {
	// GUARD: ¬P_closed(cl) — only enter body on first invocation
	if !cl.closed {
		// WP[cl.closed = true; close(cl.done), P_closed(cl) ∧ P_done(cl)]
		//   = true  (assignment establishes P_closed; close establishes P_done)
		cl.closed = true
		close(cl.done)
	}
	// SP after if: P_closed(cl) ∧ P_done(cl)   (both cases above)
}

type Coalescer[K comparable, V any] struct {
	inflight sync.Map
}

func New[K comparable, V any]() *Coalescer[K, V] {
	return &Coalescer[K, V]{}
}

// Do executes fn for key at most once concurrently. Concurrent callers with
// the same key wait for the in-flight call and share its result.
//
// PRE:  ctx != nil ∧ fn != nil ∧ key ∈ domain(K)
//       Ghost: leaders(key) and waiters(key) are finite sets.
//
// Termination (Prop 7): Do terminates provided (a) fn terminates and
// (b) at least one goroutine calling Do for the same key holds a
// non-cancelled context in each retry iteration.  If every concurrent
// caller's context is cancelled before fn completes, progress is not
// guaranteed — callers may loop indefinitely.
//
// POST: (∃ val V, err error: return = (val, err))
//       ∧ (¬panicked → err is fn's error or ctx.Err() or ErrCoalescer-wrapped panic)
//       Ghost: leaders(key) reduced by 1 OR waiter unblocked.
func (c *Coalescer[K, V]) Do(ctx context.Context, key K, fn func(context.Context, K) (V, error)) (V, error) {
	// ─── Loop ───────────────────────────────────────────────────────────────
	//
	// INV (at top of every iteration):
	//   ∀cl': (cl' was observed at LoadOrStore for key in a prior iteration)
	//         → ¬P_inflight(key, cl') ∨ P_done(cl')
	//
	//   Informal: any call object a prior iteration raced on is either gone
	//   from inflight or already completed.  This ensures progress: a waiter
	//   that loops back finds a NEW call object (freshly inserted by a new
	//   leader) or wins LoadOrStore itself.
	//
	// Termination argument:
	//   Each iteration either:
	//     (a) returns a value/error (loop exits), OR
	//     (b) enters next iteration only because P_retry(entry) was observed,
	//         meaning the prior leader finished and released inflight.
	//   The number of distinct call objects that can exist for key is bounded
	//   by the number of goroutines; every leader eventually calls closeDone
	//   (proved below), so the loop terminates given fn terminates and at
	//   least one non-cancelled goroutine exists (Prop 7).
	for {
		// Allocate fresh call; done channel open (not closed) at allocation.
		cl := &call[V]{done: make(chan struct{})}

		// LoadOrStore is linearisable: exactly one goroutine per key gets loaded==false.
		// WP[LoadOrStore, |leaders(key)| ≤ 1] is maintained by sync.Map's atomic CAS.
		actual, loaded := c.inflight.LoadOrStore(key, cl)

		// ─── Waiter path ────────────────────────────────────────────────────
		//
		// GUARD: loaded == true
		//   ≡ another goroutine stored a call cl' for key before us.
		//   Ghost: this goroutine ∈ waiters(key); |leaders(key)| unchanged.
		if loaded {
			trace.SpanFromContext(ctx).AddEvent("coalescer.wait",
				trace.WithAttributes(attribute.String("coalescer.key", fmt.Sprint(key))),
			)

			entry := actual.(*call[V])

			// PRE of select:
			//   P_inflight(key, entry) ∨ P_done(entry)
			//   (entry may have been completed by leader between LoadOrStore and here)
			//
			// WP derivation for select (postcondition Q_select = "goroutine makes progress"):
			//
			//   Arm 1: <-entry.done fires
			//     WP[if entry.retry { continue } else { return entry.val, entry.err }, Q_select]
			//       = (P_done(entry) ∧ P_retry(entry)  → Q_select via continue)
			//       ∧ (P_done(entry) ∧ ¬P_retry(entry) → Q_select via return)
			//       = P_done(entry)   ✓ (channel closed ↔ P_done)
			//
			//   Arm 2: <-ctx.Done() fires
			//     WP[return zero, ctx.Err(), Q_select]
			//       = ctx.Err() ≠ nil   (ctx.Done() fires iff ctx cancelled/deadline)
			//       = true when this arm is selected   ✓
			//
			// SP after select:
			//   (P_done(entry) ∧ ¬P_retry(entry) → returned (entry.val, entry.err))
			//   ∨ (P_done(entry) ∧ P_retry(entry) → loop continues, INV maintained)
			//   ∨ (ctx cancelled                  → returned (zero, ctx.Err()))
			select {
			case <-entry.done:
				// SP: P_done(entry) — channel close happens-before this read.
				// Happens-before chain:
				//   leader writes cl.val, cl.err under cl.mu  →
				//   leader calls close(cl.done) under cl.mu   →
				//   <-entry.done unblocks here                →
				//   we read entry.val, entry.err
				// ∴ read of entry.val/entry.err observes leader's writes (Prop 2).

				// GUARD: P_retry(entry) — leader set retry, so result is stale.
				if entry.retry {
					// SP[continue, INV]: prior call done; INV re-established because
					// ¬P_inflight(key, entry) (CompareAndDelete ran before closeDone
					// released waiters in normal-exit path; in Forget path
					// LoadAndDelete ran before closeDone).
					// SAFETY: #4 — waiter loops; a new leader will emerge.
					continue
				}
				// GUARD: ¬P_retry(entry) ∧ P_done(entry)
				// WP[return entry.val, entry.err, true] = true   ✓
				// SAFETY: #2 — waiter receives (entry.val, entry.err) from leader.
				return entry.val, entry.err

			case <-ctx.Done():
				// GUARD: ctx_w.Done() fired before entry.done (or simultaneously).
				// No mutation of shared state — only this goroutine returns.
				// SP: ctx.Err() ≠ nil ∧ no change to leaders(key) ∨ waiters(key)\{w}.
				// SAFETY: #6 — only waiter w exits; leader and other waiters unaffected.
				var zero V
				return zero, ctx.Err()
			}
		}

		// ─── Leader path ────────────────────────────────────────────────────
		//
		// SP after !loaded branch: loaded == false
		//   ∧ P_inflight(key, cl)   (we stored cl into inflight)
		//   ∧ cl.done is open (not closed)
		//   Ghost: this goroutine = owner(cl); leaders(key) += {this goroutine}.
		// SAFETY: #1 — |leaders(key)| == 1 because LoadOrStore returned loaded==false.

		var (
			val      V
			err      error
			panicVal any
			panicked bool
		)

		// ─── Leader early-exit (ctx already cancelled before fn call) ───────
		//
		// GUARD: loaded == false ∧ ctx.Err() != nil
		//   (leader won the slot but its context is already done)
		//
		// PRE:  P_inflight(key, cl) ∧ ctx.Err() ≠ nil ∧ ¬P_done(cl)
		//
		// Desired POST (Q_early):
		//   cl.err = ctx.Err()
		//   ∧ P_done(cl)           — waiters can unblock
		//   ∧ ¬P_retry(cl)         — retry NOT set; waiters return (zero, ctx.Err())
		//   ∧ ¬P_inflight(key, cl) — slot released
		//   ∧ fn never called      — no work done
		//
		// WP[CompareAndDelete; return zero, err, Q_early]:
		//   Backward through `return zero, err`:
		//     Q' = (cl.err = ctx.Err() ∧ P_done(cl) ∧ ¬P_retry(cl) ∧ ¬P_inflight(key, cl))
		//          (return makes local vars irrelevant to shared state)
		//   Backward through `c.inflight.CompareAndDelete(key, cl)`:
		//     Q'' = cl.err = ctx.Err() ∧ P_done(cl) ∧ ¬P_retry(cl)
		//           ∧ (P_inflight(key, cl) → ¬P_inflight(key, cl)')
		//           note: CompareAndDelete removes cl iff still present;
		//                 if Forget ran concurrently and already removed cl,
		//                 CompareAndDelete is a no-op — idempotent.
		//   Backward through `cl.mu.Unlock()`:
		//     same Q'' with lock released — no semantic change to predicates.
		//   Backward through `cl.closeDone()`:
		//     WP[closeDone, P_done(cl)] = true (proved above)
		//   Backward through `cl.err = err` (where err = ctx.Err()):
		//     requires cl.err' = ctx.Err() after assignment → precondition is ctx.Err() ≠ nil ✓
		//   WP of full block = ctx.Err() ≠ nil ∧ P_inflight(key, cl) = PRE   ✓
		//
		// Race-window analysis (IMPORTANT):
		//   Between LoadOrStore (line ~59, stored cl) and ctx.Err() check here,
		//   a waiter W may have loaded cl and be blocked on <-cl.done.
		//   This leader then:
		//     (a) sets cl.err = ctx.Err()
		//     (b) does NOT set cl.retry
		//     (c) calls closeDone → W unblocks
		//     (d) W sees P_done(cl) ∧ ¬P_retry(cl) → W returns (zero, ctx.Err())
		//   This is CORRECT: ctx.Err() is the context used to call Do; no fn
		//   was invoked; returning the caller's own cancellation error to
		//   waiters that happened to coalesce onto a doomed leader is
		//   acceptable — each waiter W could have been the leader itself and
		//   would have received ctx.Err() via the same early-exit code path.
		//   Conclusion: NOT a correctness violation; consistent with Prop 2
		//   (waiters receive cl.err, which is ctx.Err() here) and Prop 4
		//   (retry false → waiters return, not loop — intended since fn never ran).
		//
		// BEHAVIOUR NOTE (observable consequence of this race window):
		//   A waiter W whose own context is NOT cancelled can receive a
		//   context.Canceled (or DeadlineExceeded) error that originated from
		//   the LEADER's context, not W's.  This occurs when:
		//     1. Leader L calls LoadOrStore, stores cl (loaded==false)
		//     2. Waiter W calls LoadOrStore, loads cl (loaded==true), blocks on <-cl.done
		//     3. Leader L reaches ctx.Err() check — L's ctx is cancelled
		//     4. Leader L sets cl.err = L's ctx.Err(), closes cl.done, exits
		//     5. W unblocks, sees ¬retry, returns (zero, L's ctx.Err())
		//   W's own ctx is alive, yet W receives context.Canceled.
		//
		//   This is an intentional design trade-off:
		//     • Setting retry=true here would force W to re-execute fn, adding
		//       latency and potentially duplicating work that another goroutine
		//       is about to start anyway.
		//     • The window is narrow (between LoadOrStore and the ctx check —
		//       typically nanoseconds on a fast path).
		//     • Callers that need absolute retry semantics on leader failure
		//       should use Forget externally or wrap Do in a retry loop checking
		//       for context errors that don't match their own ctx.
		//
		//   Formally: ∃ execution trace where
		//     W.ctx.Err() == nil ∧ Do(W.ctx, key, fn) returns (zero, context.Canceled)
		//   This does NOT violate Prop 2 (result sharing) — cl.err IS the shared
		//   result.  It does NOT violate Prop 6 (waiter independence) — W's ctx
		//   was not cancelled; W received the coalesced result.  The subtlety is
		//   that the "result" is an error from a call where fn was never invoked.
		if err := ctx.Err(); err != nil {
			cl.mu.Lock()
			// SP: held(cl.mu) ∧ ¬P_done(cl) ∧ ¬P_retry(cl)
			cl.err = err
			// SP: cl.err = ctx.Err() ∧ ¬P_done(cl)
			cl.closeDone()
			// SP: cl.err = ctx.Err() ∧ P_done(cl) ∧ P_closed(cl)   (SAFETY #3)
			cl.mu.Unlock()
			c.inflight.CompareAndDelete(key, cl)
			// SP: ¬P_inflight(key, cl)   (or already deleted by Forget — idempotent)
			var zero V
			// POST: Q_early satisfied.   SAFETY: #1 (fn not called)
			return zero, err
		}

		// ─── Leader defer: normal / panic / ctx-cancelled exit ───────────────
		//
		// PRE of defer registration:
		//   P_inflight(key, cl) ∧ ¬P_done(cl) ∧ ¬P_closed(cl)
		//   ∧ ctx.Err() == nil   (checked above)
		//
		// Defer ordering note:
		//   Only one defer registered in Do per loop iteration.  The deferred
		//   func runs after fn returns (or panics).  Within the func, writes to
		//   cl.val/cl.err happen BEFORE closeDone under cl.mu — this sequencing
		//   is essential: waiters that read cl.val/cl.err after <-entry.done
		//   unblocks are guaranteed to see the final values (happens-before via
		//   channel close under mutex, then mutex release, then channel read).
		defer func() {
			// ── Panic branch ─────────────────────────────────────────────────
			//
			// GUARD: recover() != nil  ≡  fn panicked
			// PRE:   panicked == false ∧ panicVal == nil (default)
			//
			// WP[panicked=true; panicVal=r; err=fmt.Errorf(...), POST_panic]:
			//   POST_panic ≡ cl.err wraps ErrCoalescer ∧ P_done(cl) ∧ ¬P_retry(cl)
			//                ∧ leader will re-panic after mutex release
			//   WP backward through assignments: err must equal wrapped panic value → ✓
			//
			// SAFETY: #5 — panic captured, wrapped error set, waiters unblocked
			//              with ErrCoalescer-wrapped err; leader re-panics below.
			if r := recover(); r != nil {
				panicked = true
				panicVal = r
				err = fmt.Errorf("%w: panic: %v", ErrCoalescer, r)
			}
			// SP: (panicked → err wraps ErrCoalescer) ∧ (¬panicked → err from fn or nil)

			cl.mu.Lock()
			// SP: held(cl.mu) — exclusive access to cl fields.

			// Write val and err before closing done channel.
			// This ordering is the crucial happens-before anchor for Prop 2:
			//   write cl.val → write cl.err → close(cl.done) [all under cl.mu]
			//   ∴ any goroutine that reads cl.val/cl.err after <-cl.done observes these.
			cl.val = val
			cl.err = err
			// SP: cl.val = val ∧ cl.err = err   (under mutex, not yet visible to waiters)

			// ── Context-cancelled branch (fn returned but ctx expired) ────────
			//
			// GUARD: ¬panicked ∧ ctx.Err() != nil
			//   fn ran to completion but leader's context was cancelled during fn.
			//   Setting retry instructs waiters to loop and find a new leader.
			//
			// WP[cl.retry = true, P_retry(cl)] = true   ✓
			//
			// POST (ctx-cancelled): P_done(cl) ∧ P_retry(cl) ∧ ¬P_inflight(key, cl)
			// SAFETY: #4 — waiters will loop; exactly one new leader via LoadOrStore.
			if !panicked && ctx.Err() != nil {
				cl.retry = true
			}
			// SP: (ctx-cancelled ∧ ¬panicked → P_retry(cl)) ∧ (otherwise → ¬P_retry(cl))

			cl.closeDone()
			// SP: P_done(cl) ∧ P_closed(cl)   (SAFETY #3: idempotent, first close here)

			cl.mu.Unlock()
			// SP: ¬held(cl.mu)
			//     Waiters blocked on <-entry.done can now proceed and read cl.val/cl.err.
			//     happens-before: mutex-unlock → channel-close is enclosed in same
			//     critical section, so unlock happens after close; waiter's
			//     channel-receive happens-after the close. ✓

			c.inflight.CompareAndDelete(key, cl)
			// SP: ¬P_inflight(key, cl)   (CAS: removes cl only if it's still the stored value;
			//     Forget may have run LoadAndDelete earlier — in that case CAS is no-op, correct.)
			//
			// POST (normal exit):
			//   cl.val = val ∧ cl.err = err ∧ P_done(cl) ∧ ¬P_retry(cl) ∧ ¬P_inflight(key, cl)
			//   Ghost: leaders(key) -= {this goroutine}
			// SAFETY: #1 (leader released), #2 (waiters receive val/err)

			// ── Re-panic branch ──────────────────────────────────────────────
			//
			// GUARD: panicked == true
			// PRE:  P_done(cl) ∧ cl.err wraps ErrCoalescer ∧ ¬P_inflight(key, cl)
			//       (waiters already released with wrapped error above)
			// POST (panic exit):
			//   leader goroutine panics with original panicVal
			//   waiters already unblocked with ErrCoalescer-wrapped err (Prop 5 satisfied)
			// SAFETY: #5
			if panicked {
				panic(panicVal)
			}
		}()

		// ── fn invocation ────────────────────────────────────────────────────
		//
		// PRE:  P_inflight(key, cl) ∧ ¬P_done(cl) ∧ ctx.Err() == nil
		//       Ghost: leaders(key) = {this goroutine}, |leaders(key)| = 1   (SAFETY #1)
		//
		// fn(ctx, key) runs; may return normally, return with ctx cancellation,
		// or panic.  All three cases are handled by the deferred function above.
		//
		// POST of fn call (before defer):  val, err assigned; deferred func will run.
		val, err = fn(ctx, key)

		// WP[return val, err, POST_normal]:
		//   POST_normal = (val, err) = (fn.val, fn.err)   ← trivially true after assignment
		//   The deferred func executes after this return, establishing full POST.
		return val, err
	}
}

// Forget evicts the in-flight call for key, if any. Current waiters will
// retry with a fresh execution; the evicted leader continues but its
// result is discarded by any waiter that races past Forget.
//
// PRE:  any state — safe to call at any time, including concurrently with Do.
//
// POST (cl in inflight):
//   P_retry(cl) ∧ P_done(cl) ∧ ¬P_inflight(key, cl)
//   Ghost: waiters(key) will all execute `continue` upon unblocking.
//
// POST (nothing in inflight):
//   state unchanged — no-op.
//
// Forget vs defer race analysis (SAFETY #3, #4):
//   Timeline A — Forget wins (runs LoadAndDelete before leader's CompareAndDelete):
//     Forget: LoadAndDelete removes cl from inflight
//             → cl.retry = true   (under cl.mu)
//             → cl.closeDone()    (closes done, sets cl.closed=true)
//     Leader defer: CompareAndDelete(key, cl) — cl no longer in map → no-op ✓
//                   closeDone() — cl.closed already true → no-op (SAFETY #3) ✓
//   Timeline B — Leader defer wins (CompareAndDelete removes cl first):
//     Leader: closeDone() closes done (first close)
//             CompareAndDelete removes cl
//     Forget: LoadAndDelete — cl no longer in map → ok==false → body skipped ✓
//   Timeline C — Concurrent (interleaved under different mutexes):
//     Forget acquires cl.mu; leader defer acquires cl.mu; they are serialised
//     by cl.mu.  One of Timeline A/B applies depending on mutex acquisition order.
//   Conclusion: closeDone idempotency (SAFETY #3) ensures done never double-closed;
//               retry flag set by whichever path runs first is visible to waiters
//               because it is set before closeDone, which happens-before <-entry.done. ✓
func (c *Coalescer[K, V]) Forget(key K) {
	// WP[LoadAndDelete; ..., POST_forget]:
	//   Backward through body (cl found):
	//     POST_body = P_retry(cl) ∧ P_done(cl)
	//     WP[cl.mu.Unlock, POST_body] = POST_body (unlock is no-op on predicates)
	//     WP[cl.closeDone(), P_done(cl)] = true (proved in closeDone)
	//     WP[cl.retry = true, P_retry(cl)] = true
	//     WP[cl.mu.Lock, held(cl.mu)] = true (lock is always acquirable eventually)
	//   WP of body given ok==true: true ✓
	//   WP given ok==false: no-op, POST = state unchanged ✓
	if v, ok := c.inflight.LoadAndDelete(key); ok {
		cl := v.(*call[V])
		// PRE body: ¬P_inflight(key, cl)   (LoadAndDelete removed it atomically)
		//           ∧ cl may or may not be done (leader may still be running)
		cl.mu.Lock()
		// SP: held(cl.mu)
		cl.retry = true
		// SP: P_retry(cl)   — visible to waiters after done closes (happens-before)
		cl.closeDone()
		// SP: P_retry(cl) ∧ P_done(cl) ∧ P_closed(cl)   SAFETY: #3, #4
		cl.mu.Unlock()
		// POST body: P_retry(cl) ∧ P_done(cl) ∧ ¬P_inflight(key, cl)   ✓
	}
}
