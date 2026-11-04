package thread_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// recordingScheduler collects what a session hands it.
type recordingScheduler struct {
	mu  sync.Mutex
	got []thread.Ready
	ch  chan thread.Ready
}

func newRecordingScheduler() *recordingScheduler {
	return &recordingScheduler{ch: make(chan thread.Ready, 64)}
}

func (r *recordingScheduler) Schedule(x thread.Ready) {
	r.mu.Lock()
	r.got = append(r.got, x)
	r.mu.Unlock()
	r.ch <- x
}

// next waits for the next notification of kind.
func (r *recordingScheduler) next(t *testing.T, kind thread.ReadyKind) thread.Ready {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case x := <-r.ch:
			if x.Kind == kind {
				return x
			}
		case <-deadline:
			t.Fatalf("no %s notification", kind)
		}
	}
}

func shipAgent(turns ...wefttest.Turn) *core.Agent {
	ship := core.Tool("ship", "Ship an order.",
		func(ctx context.Context, in refundInput) (string, error) { return "shipped " + in.OrderID, nil },
		core.RequireApproval())
	return core.New(wefttest.Script(turns...), core.Name("sched"), ship)
}

// Under OnReady the session starts nothing nobody asked for: the
// decision that settles a boundary announces the resume instead of
// running it, the send queued behind the boundary is announced when
// the resume ends instead of drained, and each starts when the
// scheduler calls Resume or Continue.
func TestOnReadyOwnsResumeAndQueue(t *testing.T) {
	sch := newRecordingScheduler()
	s, err := thread.Create(bg, thread.Memory(), shipAgent(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`}),
		wefttest.Say("shipped"),
		wefttest.Say("second answer"),
	), thread.OnReady(sch.Schedule))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	queued, err := s.Send(bg, core.User("and then?"))
	if err != nil {
		t.Fatal(err)
	}
	rt, err := s.Decide(bg, bound(s, thread.Approve("call_1")))
	if err != nil {
		t.Fatal(err)
	}
	if rt != nil {
		t.Fatalf("Decide armed a resume under OnReady")
	}
	r := sch.next(t, thread.ReadyResume)
	if r.Session != s.ID() || r.RunID != s.ID()+"-t1" {
		t.Fatalf("resume notification = %+v", r)
	}
	if err := s.WaitIdle(bg); err != nil {
		t.Fatal(err)
	}
	if got := s.Ready(); len(got) != 1 || got[0].Kind != thread.ReadyResume {
		t.Fatalf("Ready() = %+v", got)
	}
	rt, err = s.Resume(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	q := sch.next(t, thread.ReadyQueued)
	if q.RunID != queued.RunID() {
		t.Fatalf("queued notification = %+v, want run %s", q, queued.RunID())
	}
	if err := s.WaitIdle(bg); err != nil {
		t.Fatal(err)
	}
	select {
	case <-queued.Done():
		t.Fatal("the queued send ran without the scheduler's Continue")
	default:
	}
	ct, err := s.Continue(bg)
	if err != nil {
		t.Fatal(err)
	}
	if ct != queued {
		t.Fatalf("Continue returned %v, want the queued turn", ct)
	}
	res, err := queued.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Text() != "second answer" {
		t.Fatalf("queued turn answered %q", res.Text())
	}
	if err := s.Close(bg); err != nil {
		t.Fatal(err)
	}
}

// After a restart the scheduler's boot recovery asks Ready: a boundary
// decided before the crash is listed with its parked run and version,
// and the send queued behind it follows once the resume ends. Both
// file backends, a fresh store for the reopen.
func TestOnReadyReadyAfterRestart(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			s, err := thread.Create(bg, st, shipAgent(
				wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`}),
			), thread.OnReady(newRecordingScheduler().Schedule))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("ship it"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			queued, err := s.Send(bg, core.User("then?"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Decide(bg, bound(s, thread.Approve("call_1"))); err != nil {
				t.Fatal(err)
			}
			id := s.ID()
			st = restart(t, b, st, id)

			sch := newRecordingScheduler()
			s2, err := thread.Open(bg, st, id, shipAgent(wefttest.Say("shipped"), wefttest.Say("next")),
				thread.OnReady(sch.Schedule))
			if err != nil {
				t.Fatal(err)
			}
			got := s2.Ready()
			if len(got) != 1 || got[0].Kind != thread.ReadyResume || got[0].RunID != id+"-t1" {
				t.Fatalf("Ready() after restart = %+v", got)
			}
			rt, err := s2.Resume(bg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			q := sch.next(t, thread.ReadyQueued)
			if q.RunID != queued.RunID() {
				t.Fatalf("queued after restart = %+v, want %s", q, queued.RunID())
			}
			ct, err := s2.Continue(bg)
			if err != nil || ct == nil {
				t.Fatalf("Continue = %v, %v", ct, err)
			}
			if _, err := ct.Wait(); err != nil {
				t.Fatal(err)
			}
			if err := s2.WaitIdle(bg); err != nil {
				t.Fatal(err)
			}
			if got := s2.Ready(); len(got) != 0 {
				t.Fatalf("Ready() when done = %+v", got)
			}
			if err := s2.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// AutoResume(false) without OnReady keeps its contract: nothing
// resumes until Resume, and Ready lists the decided boundary.
func TestAutoResumeOffReady(t *testing.T) {
	s, err := thread.Create(bg, thread.Memory(), shipAgent(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`}),
		wefttest.Say("done"),
	), thread.AutoResume(false))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(bg, core.User("ship it"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WaitIdle(bg); err != nil {
		t.Fatal(err)
	}
	_ = turn
	if got := s.Ready(); len(got) != 0 {
		t.Fatalf("Ready() while undecided = %+v", got)
	}
	if rt, err := s.Decide(bg, bound(s, thread.Approve("call_1"))); err != nil || rt != nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if err := s.WaitIdle(bg); err != nil { // the runner may still be between items
		t.Fatal(err)
	}
	if got := s.Ready(); len(got) != 1 || got[0].Kind != thread.ReadyResume {
		t.Fatalf("Ready() = %+v", got)
	}
}

// Close does not wait on queued work an OnReady owner never started: the
// queued turn ends with ErrClosed, its receipt stays for the next Open.
func TestOnReadyCloseWithQueued(t *testing.T) {
	st := thread.Memory()
	sch := newRecordingScheduler()
	s, err := thread.Create(bg, st, shipAgent(
		wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`}),
		wefttest.Say("shipped"),
	), thread.OnReady(sch.Schedule))
	if err != nil {
		t.Fatal(err)
	}
	turn, _ := s.Send(bg, core.User("ship it"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	queued, _ := s.Send(bg, core.User("then?"))
	if _, err := s.Decide(bg, bound(s, thread.Approve("call_1"))); err != nil {
		t.Fatal(err)
	}
	sch.next(t, thread.ReadyResume)
	rt, _ := s.Resume(bg)
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	sch.next(t, thread.ReadyQueued)
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if _, err := queued.Wait(); err == nil {
		t.Fatal("the never-started queued turn reports success")
	}
	s2, err := thread.Open(bg, st, s.ID(), shipAgent())
	if err != nil {
		t.Fatal(err)
	}
	if q := s2.Queue(); len(q) != 1 {
		t.Fatalf("restored queue = %+v", q)
	}
}

// A boundary whose only undecided request has lapsed is ready to
// resume: Ready lists it — reading only, nothing is denied by asking —
// and Resume denies the lapsed call first, then runs. Both file
// backends, across a restart.
func TestReadyWithOnlyExpiredRequests(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var now atomic.Int64
			now.Store(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC).UnixNano())
			clock := thread.Clock(func() time.Time { return time.Unix(0, now.Load()).UTC() })
			st := b.open(t)
			s, err := thread.Create(bg, st, shipAgent(
				wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`}),
			), thread.RequestExpiry(time.Minute), clock, thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("ship it"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			if got := s.Ready(); len(got) != 0 {
				t.Fatalf("Ready() with a live request = %+v", got)
			}
			id := s.ID()
			st = restart(t, b, st, id)
			now.Add(int64(2 * time.Minute))
			s2, err := thread.Open(bg, st, id, shipAgent(wefttest.Say("not shipped")), clock, thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			before := len(s2.Entries())
			got := s2.Ready()
			if len(got) != 1 || got[0].Kind != thread.ReadyResume || got[0].RunID != id+"-t1" {
				t.Fatalf("Ready() with only a lapsed request = %+v", got)
			}
			if len(s2.Entries()) != before {
				t.Fatal("Ready wrote to the session")
			}
			rt, err := s2.Resume(bg)
			if err != nil {
				t.Fatal(err)
			}
			res, err := rt.Wait()
			if err != nil || res.Text() != "not shipped" {
				t.Fatalf("resume = %v, %v", res, err)
			}
		})
	}
}

// A cut-off turn is listed alone, as ReadyRecover naming the cut-off
// run: a boundary behind it may be a resume the crash cut short. Boot
// recovery is Open → Claim → Ready → act; after RecoverTurn the next
// work is listed. Both file backends.
func TestReadyListsACutOffTurnAlone(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				charge.crash = cs
				return core.New(wefttest.Script(charged("charge")), charge.def())
			}, nil)
			charge.crash = nil
			st = restart(t, b, st, id)
			sch := newRecordingScheduler()
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("ok")), charge.def()), thread.OnReady(sch.Schedule))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			if err := s.Claim(bg); err != nil {
				t.Fatal(err)
			}
			got := s.Ready()
			if len(got) != 1 || got[0].Kind != thread.ReadyRecover || got[0].RunID != id+"-t1" {
				t.Fatalf("Ready() after the crash = %+v", got)
			}
			if _, err := s.RecoverTurn(bg); err != nil {
				t.Fatal(err)
			}
			if got := s.Ready(); len(got) != 0 {
				t.Fatalf("Ready() with the recovery request undecided = %+v", got)
			}
			if _, err := s.Decide(bg, bound(s, thread.Deny("call_1", "skip"))); err != nil {
				t.Fatal(err)
			}
			r := sch.next(t, thread.ReadyResume)
			if r.RunID != id+"-t1" {
				t.Fatalf("hint after the decision = %+v", r)
			}
		})
	}
}
