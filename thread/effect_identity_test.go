package thread_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// Effect identity (ADR 0032, the 2026-11-10 amendment): once a call's
// outcome is unknown in a session, a later call to the same tool with
// the same canonical arguments runs under the earlier call's key; and
// while an abandoned handler of a tool still runs in this process, a
// new call to that tool parks for a person, whatever its arguments.

// keyedShip is a tool that records the key of every handler it starts
// and how many ran at once; its first handler blocks until release
// (ignoring its context: the loop abandons it at its Timeout).
type keyedShip struct {
	mu      sync.Mutex
	keys    []string
	active  int
	most    int
	release chan struct{}
}

func newKeyedShip() *keyedShip { return &keyedShip{release: make(chan struct{})} }

func (k *keyedShip) def(name string, policy core.ReplayPolicy, timeout time.Duration) *core.ToolDef {
	type in struct {
		Order any `json:"order"`
	}
	return core.Tool(name, "Ship an order.", func(ctx context.Context, _ in) (string, error) {
		c, _ := core.CallFromContext(ctx)
		k.mu.Lock()
		k.keys = append(k.keys, c.IdempotencyKey)
		first := len(k.keys) == 1
		k.active++
		k.most = max(k.most, k.active)
		k.mu.Unlock()
		defer func() {
			k.mu.Lock()
			k.active--
			k.mu.Unlock()
		}()
		if first {
			<-k.release
		}
		return "shipped", nil
	}, core.Replay(policy), core.Timeout(timeout))
}

func (k *keyedShip) snapshot() (keys []string, most int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.keys...), k.most
}

// effectStore is one backend an effect-identity test runs on: open
// returns the store for a new session, reopen the store a restart (or,
// for memory, a Close and Open in the same process) reads it back from.
type effectStore struct {
	name   string
	open   func(t *testing.T) thread.Storage
	reopen func(t *testing.T, st thread.Storage, id string) thread.Storage
}

func effectStores(t *testing.T) []effectStore {
	t.Helper()
	out := []effectStore{{
		name:   "memory",
		open:   func(*testing.T) thread.Storage { return thread.Memory() },
		reopen: func(_ *testing.T, st thread.Storage, _ string) thread.Storage { return st },
	}}
	for _, b := range durableBackends(t) {
		out = append(out, effectStore{
			name:   b.name,
			open:   b.open,
			reopen: func(t *testing.T, st thread.Storage, id string) thread.Storage { return restart(t, b, st, id) },
		})
	}
	return out
}

// abandonFirstShip runs one turn whose ship call s1 (args) outlives its
// Timeout: the turn ends with s1 parked as a recovery request, its
// handler still running. ok is false when the call timed out in the
// dispatch gate instead (outlive doubles the Timeout and tries again).
func abandonFirstShip(t *testing.T, s *thread.Session, ship *keyedShip) (thread.Request, bool) {
	t.Helper()
	turn, err := s.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	if keys, _ := ship.snapshot(); len(keys) == 0 {
		return thread.Request{}, false
	}
	p := s.Pending()
	if len(p) != 1 || !p[0].Recovery || !strings.Contains(p[0].Reason, "still running") {
		t.Fatalf("Pending = %+v, want s1's recovery request, its handler still running", p)
	}
	return p[0], true
}

// returnHandler lets the abandoned handler of r's call return and
// waits until the session sees it returned (the registry releases it
// in core.AfterDispatch).
func returnHandler(ship *keyedShip, r thread.Request) {
	done := thread.AbandonedHandler(r.RunID, r.CallID)
	close(ship.release)
	if done != nil {
		<-done
	}
}

// repeatTimedOut reports whether call id parked because its own
// handler outlived the test's short Timeout — an unknown outcome, a
// recovery request: a timing artifact of a loaded machine, and outlive
// tries again with twice the Timeout. A park with the still-running
// gate's reason after the first handler returned is a failure.
func repeatTimedOut(t *testing.T, s *thread.Session, id string) bool {
	t.Helper()
	for _, r := range s.Pending() {
		if r.CallID != id {
			continue
		}
		if r.Recovery {
			return true
		}
		if strings.HasPrefix(r.Reason, "an earlier call of this tool is still running") {
			t.Fatalf("%s parked by the still-running gate after the first handler returned: %+v", id, r)
		}
	}
	return false
}

// keyRecords returns the session's key records (StepKey).
func keyRecords(s *thread.Session) []thread.ApprovalAuditEntry {
	var out []thread.ApprovalAuditEntry
	for _, e := range s.Entries() {
		if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepKey {
			out = append(out, a)
		}
	}
	return out
}

// A repeat with the same arguments — 1.0 is 1 — after a person skipped
// the recovery request runs under the first call's key, recorded.
func TestRepeatAfterSkipInheritsTheKey(t *testing.T) {
	for _, b := range effectStores(t) {
		t.Run(b.name, func(t *testing.T) {
			outlive(t, 10*time.Millisecond, func(timeout time.Duration) bool {
				ship := newKeyedShip()
				s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`}),
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s2", Args: `{ "order" : 1.0 }`}),
					wefttest.Say("done"),
				), ship.def("ship", core.ReplayNever, timeout)))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.Close(bg) }()
				r, ok := abandonFirstShip(t, s, ship)
				if !ok {
					return false
				}
				returnHandler(ship, r)
				rt, err := s.Decide(bg, thread.Deny("s1", "checked by hand").Bind(r))
				if err != nil || rt == nil {
					t.Fatalf("Decide(deny) = %v, %v", rt, err)
				}
				res, err := rt.Wait()
				if err != nil {
					t.Fatal(err)
				}
				if gateTimedOut(s) || repeatTimedOut(t, s, "s2") {
					return false
				}
				if res.Text() != "done" {
					t.Fatalf("resume = %v", res)
				}
				keys, most := ship.snapshot()
				if len(keys) != 2 || keys[1] != keys[0] || keys[0] != r.IdempotencyKey || most != 1 {
					t.Fatalf("keys %v (request key %s), at most %d at once; want the repeat under the first key", keys, r.IdempotencyKey, most)
				}
				recs := keyRecords(s)
				if len(recs) != 1 || recs[0].Key != keys[0] || recs[0].CallID != "s2" || recs[0].Tool != "ship" {
					t.Fatalf("key records = %+v, want one naming s2 under %s", recs, keys[0])
				}
				return true
			})
		})
	}
}

// A repeat with other arguments parks while the first handler runs —
// an approval is refused — and, once it has returned, runs under a
// key of its own. With the same arguments it parks too, and then runs
// under the first call's key: the gate lifts, the key is inherited.
func TestRepeatParksWhileTheFirstHandlerRuns(t *testing.T) {
	for _, tc := range []struct {
		name, args string
		inherit    bool
	}{
		{"other-args", `{"order":2}`, false},
		{"same-args", `{"order":1}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, b := range effectStores(t) {
				t.Run(b.name, func(t *testing.T) {
					outlive(t, 10*time.Millisecond, func(timeout time.Duration) bool {
						ship := newKeyedShip()
						released := false
						defer func() {
							if !released {
								close(ship.release)
							}
						}()
						s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(
							wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`}),
							wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s2", Args: tc.args}),
							wefttest.Say("done"),
						), ship.def("ship", core.ReplayNever, timeout)))
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = s.Close(bg) }()
						r, ok := abandonFirstShip(t, s, ship)
						if !ok {
							return false
						}
						// Skip s1 while its handler still runs: the model asks
						// for ship again, and the gate parks it.
						rt, err := s.Decide(bg, thread.Deny("s1", "").Bind(r))
						if err != nil || rt == nil {
							t.Fatalf("Decide(deny) = %v, %v", rt, err)
						}
						if _, err := rt.Wait(); err != nil {
							t.Fatal(err)
						}
						// s2's Timeout spans the dispatch gate: on slow storage
						// it can pass while the gate records the park, and s2 is
						// answered "timed out" — nothing parked. A timing
						// artifact (outlive): again, with twice the Timeout.
						if gateTimedOut(s) {
							return false
						}
						if keys, _ := ship.snapshot(); len(keys) != 1 {
							t.Fatalf("ship started %d handlers while the first ran, want 1", len(keys))
						}
						p := s.Pending()
						if len(p) != 1 || p[0].CallID != "s2" || p[0].Recovery ||
							p[0].Reason != "an earlier call of this tool is still running: its handler was abandoned and has not returned; approve once it has returned, deny to skip this call" {
							t.Fatalf("Pending = %+v, want s2 held by the still-running gate", p)
						}
						if rt, err := s.Decide(bg, thread.Approve("s2").Bind(p[0])); !errors.Is(err, thread.ErrStillRunning) || rt != nil {
							t.Fatalf("Decide(approve) while the first handler runs = %v, %v; want ErrStillRunning", rt, err)
						}
						returnHandler(ship, r)
						released = true
						rt, err = s.Decide(bg, thread.Approve("s2").Bind(p[0]))
						if err != nil || rt == nil {
							t.Fatalf("Decide(approve) after the handler returned = %v, %v", rt, err)
						}
						res, err := rt.Wait()
						if err != nil {
							t.Fatal(err)
						}
						if gateTimedOut(s) || repeatTimedOut(t, s, "s2") {
							return false
						}
						if res.Text() != "done" {
							t.Fatalf("resume = %v", res)
						}
						keys, most := ship.snapshot()
						if len(keys) != 2 || most != 1 || (keys[1] == keys[0]) != tc.inherit {
							t.Fatalf("keys %v, at most %d at once; inherit=%v", keys, most, tc.inherit)
						}
						return true
					})
				})
			}
		})
	}
}

// The inheritance is the file's: a restart (a reopen, for memory)
// keeps it — the repeat in a later turn runs under the first call's
// key — and an answer under that key closes it: the next identical
// call, a deliberate second order, is a new effect.
func TestInheritanceSurvivesARestartAndClosesOnAnAnswer(t *testing.T) {
	for _, b := range effectStores(t) {
		t.Run(b.name, func(t *testing.T) {
			outlive(t, 10*time.Millisecond, func(timeout time.Duration) bool {
				ship := newKeyedShip()
				st := b.open(t)
				s, err := thread.Create(bg, st, core.New(wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`}),
					wefttest.Say("skipped"),
				), ship.def("ship", core.ReplayNever, timeout)))
				if err != nil {
					t.Fatal(err)
				}
				id := s.ID()
				r, ok := abandonFirstShip(t, s, ship)
				if !ok {
					_ = s.Close(bg)
					return false
				}
				returnHandler(ship, r)
				rt, err := s.Decide(bg, thread.Deny("s1", "").Bind(r))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rt.Wait(); err != nil {
					t.Fatal(err)
				}
				if err := s.Close(bg); err != nil {
					t.Fatal(err)
				}
				st = b.reopen(t, st, id)
				s, err = thread.Open(bg, st, id, core.New(wefttest.Script(
					// The same call id in a later turn, and a second order.
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`}),
					wefttest.Say("shipped again"),
					wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`}),
					wefttest.Say("shipped a second order"),
				), ship.def("ship", core.ReplayNever, time.Minute)))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.Close(bg) }()
				for _, prompt := range []string{"ship it again", "ship another one"} {
					turn, err := s.Send(bg, core.User(prompt))
					if err != nil {
						t.Fatal(err)
					}
					if _, err := turn.Wait(); err != nil {
						t.Fatal(err)
					}
				}
				keys, _ := ship.snapshot()
				if len(keys) != 3 || keys[1] != keys[0] || keys[2] == keys[0] {
					t.Fatalf("keys %v: want the repeat under the first key, the second order under a new one", keys)
				}
				return true
			})
		})
	}
}

// A Fork carries the origin's open unknown outcomes with the origin's
// keys: a repeat in the fork runs under the origin's key, and so does
// a person's retry there of a recovery request the fork copied.
func TestForkInheritsOpenUnknownOutcomes(t *testing.T) {
	for _, mode := range []string{"repeat-after-skip", "retry-copied-request"} {
		t.Run(mode, func(t *testing.T) {
			for _, b := range effectStores(t) {
				t.Run(b.name, func(t *testing.T) {
					outlive(t, 10*time.Millisecond, func(timeout time.Duration) bool {
						ship := newKeyedShip()
						// Fork runs the origin's agent: one script for both.
						steps := []wefttest.Turn{wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`})}
						if mode == "repeat-after-skip" {
							steps = append(steps, wefttest.Say("skipped"),
								wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "f1", Args: `{"order":1}`}))
						}
						steps = append(steps, wefttest.Say("done"))
						s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(steps...),
							ship.def("ship", core.ReplayNever, timeout)))
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = s.Close(bg) }()
						r, ok := abandonFirstShip(t, s, ship)
						if !ok {
							return false
						}
						returnHandler(ship, r)
						if mode == "repeat-after-skip" {
							rt, err := s.Decide(bg, thread.Deny("s1", "").Bind(r))
							if err != nil {
								t.Fatal(err)
							}
							if _, err := rt.Wait(); err != nil {
								t.Fatal(err)
							}
						}
						f, err := s.Fork(bg, s.Leaf())
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = f.Close(bg) }()
						var turn *thread.Turn
						if mode == "repeat-after-skip" {
							turn, err = f.Send(bg, core.User("ship it in the fork"))
						} else {
							fp := f.Pending()
							if len(fp) != 1 || !fp[0].Recovery || fp[0].IdempotencyKey != r.IdempotencyKey {
								t.Fatalf("fork Pending = %+v, want the copied recovery request under the origin's key %s", fp, r.IdempotencyKey)
							}
							turn, err = f.Decide(bg, thread.Approve("s1").Bind(fp[0]))
						}
						if err != nil {
							t.Fatal(err)
						}
						if _, err := turn.Wait(); err != nil {
							t.Fatal(err)
						}
						if gateTimedOut(f) || repeatTimedOut(t, f, "f1") || repeatTimedOut(t, f, "s1") {
							return false
						}
						keys, _ := ship.snapshot()
						if len(keys) != 2 || keys[1] != keys[0] {
							t.Fatalf("keys %v: the fork's call must run under the origin's key", keys)
						}
						return true
					})
				})
			}
		})
	}
}

// A ReplaySafe tool is untouched: its abandoned handler is not filed,
// its repeat neither parks nor inherits, and no key is recorded.
func TestReplaySafeIsNeitherGatedNorInherited(t *testing.T) {
	for _, b := range effectStores(t) {
		t.Run(b.name, func(t *testing.T) {
			ship := newKeyedShip()
			defer close(ship.release)
			s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "lookup", ID: "l1", Args: `{"order":1}`}),
				wefttest.ToolCalls(wefttest.Call{Name: "lookup", ID: "l2", Args: `{"order":1}`}),
				wefttest.Say("done"),
			), ship.def("lookup", core.ReplaySafe, 20*time.Millisecond)))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("look it up"))
			if err != nil {
				t.Fatal(err)
			}
			if res, err := turn.Wait(); err != nil || res.Text() != "done" {
				t.Fatalf("turn = %v, %v", res, err)
			}
			if p := s.Pending(); len(p) != 0 {
				t.Fatalf("Pending = %+v, want none", p)
			}
			keys, most := ship.snapshot()
			if len(keys) != 2 || keys[0] == keys[1] || most != 2 {
				t.Fatalf("keys %v, at most %d at once: want two independent runs beside each other", keys, most)
			}
			if recs := keyRecords(s); len(recs) != 0 {
				t.Fatalf("key records = %+v, want none", recs)
			}
		})
	}
}

// The repeat arriving as the first handler returns: whichever wins, the
// repeat never runs beside the first handler — it parks, or it runs
// once the handler has returned — and when it runs it runs under the
// first call's key. Run with -race -count=50 and make stress.
func TestRepeatRacingTheHandlersReturn(t *testing.T) {
	outlive(t, 10*time.Millisecond, func(timeout time.Duration) bool {
		ship := newKeyedShip()
		s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s1", Args: `{"order":1}`}),
			wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "s2", Args: `{"order":1}`}),
			wefttest.Say("done"),
			wefttest.Say("done"),
		), ship.def("ship", core.ReplayNever, timeout)))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close(bg) }()
		r, ok := abandonFirstShip(t, s, ship)
		if !ok {
			return false
		}
		done := thread.AbandonedHandler(r.RunID, r.CallID)
		rt, err := s.Decide(bg, thread.Deny("s1", "").Bind(r))
		if err != nil {
			t.Fatal(err)
		}
		close(ship.release) // races the resume's dispatch of s2
		if _, err := rt.Wait(); err != nil {
			t.Fatal(err)
		}
		if done != nil {
			<-done
		}
		if p := s.Pending(); len(p) == 1 && p[0].Recovery {
			return false // s2's own handler outlived the short Timeout: again, with twice it
		}
		if p := s.Pending(); len(p) == 1 {
			// The gate won: s2 parked. Its handler has returned now.
			rt, err := s.Decide(bg, thread.Approve("s2").Bind(p[0]))
			if err != nil {
				t.Fatalf("Decide(approve) after the handler returned = %v", err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			if p := s.Pending(); len(p) == 1 && p[0].Recovery {
				return false // the approved s2 outlived the short Timeout
			}
		}
		if gateTimedOut(s) {
			return false
		}
		keys, most := ship.snapshot()
		if len(keys) != 2 || keys[1] != keys[0] || most != 1 {
			t.Fatalf("keys %v, at most %d at once", keys, most)
		}
		return true
	})
}
