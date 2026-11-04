package thread_test

import (
	"context"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// P0-1: a call a park rule held never started, and recovery never runs
// it unasked. One step: a ReplayKeyed pay the agent's ParkSource parks,
// a ReplayNever charge that runs and dies with the process. RecoverTurn
// knows from the dispatch records that pay never started: it parks as
// an ordinary request, not a keyed retry; skipping charge leaves pay
// waiting, and it never runs. Both file backends, across a restart.
func TestRecoverNeverRunsAParkedCall(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			pay := &sideEffect{name: "pay"}
			tools := func() []core.Option {
				return []core.Option{charge.def(), pay.def(core.Replay(core.ReplayKeyed)),
					core.ParkSource(func(context.Context) []string { return []string{"pay"} })}
			}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				charge.crash = cs
				return core.New(wefttest.Script(wefttest.ToolCalls(
					wefttest.Call{Name: "pay", ID: "c_pay", Args: `{"order_id":"1"}`},
					wefttest.Call{Name: "charge", ID: "c_charge", Args: `{"order_id":"1"}`},
				)), tools()...)
			}, nil)
			charge.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")), tools()...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			if rt, err := s.RecoverTurn(bg); err != nil || rt != nil {
				t.Fatalf("RecoverTurn = %v, %v; want both calls waiting", rt, err)
			}
			p := s.Pending()
			if len(p) != 2 {
				t.Fatalf("Pending = %+v, want pay and charge", p)
			}
			for _, r := range p {
				switch r.CallID {
				case "c_pay":
					if r.Recovery || r.Reason != "cut off before it started: approve to run it, deny to skip it" {
						t.Fatalf("pay's request = %+v, want an ordinary request", r)
					}
				case "c_charge":
					if !r.Recovery {
						t.Fatalf("charge's request = %+v, want a recovery request", r)
					}
				}
			}
			for _, e := range s.Audit() {
				if d, ok := e.(thread.ApprovalDecisionEntry); ok && d.CallID == "c_pay" {
					t.Fatalf("the session decided the parked call: %+v", d)
				}
			}
			var charged thread.Request
			for _, r := range p {
				if r.CallID == "c_charge" {
					charged = r
				}
			}
			if rt, err := s.Decide(bg, thread.Deny("c_charge", "skip").Bind(charged)); err != nil || rt != nil {
				t.Fatalf("Decide(skip charge) = %v, %v; want pay still waiting", rt, err)
			}
			if n := len(pay.ran()); n != 0 {
				t.Fatalf("the parked pay ran %d times with no approval", n)
			}
		})
	}
}

// legacyStore writes like a release before dispatch records: no step
// is marked DispatchRecorded and no dispatch record lands.
type legacyStore struct{ thread.Storage }

func (l legacyStore) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	var out []thread.Entry
	for _, e := range entries {
		switch x := e.(type) {
		case thread.MessageEntry:
			x.DispatchRecorded = false
			e = x
		case thread.ApprovalAuditEntry:
			if x.Step == thread.StepDispatch {
				continue
			}
		}
		out = append(out, e)
	}
	if len(out) == 0 {
		return nil
	}
	return l.Storage.Append(ctx, session, out...)
}

// A step from before dispatch records: whether a ReplayKeyed call was
// parked is not knowable, so RecoverTurn never retries it on its own —
// it parks as a recovery request for a person. Both backends.
func TestRecoverUnknowableStartGoesToAPerson(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &sideEffect{name: "pay"}
			inner := b.open(t)
			cs := &crashStore{Storage: legacyStore{inner}}
			pay.crash = cs
			s, err := thread.Create(bg, cs, core.New(wefttest.Script(charged("pay")), pay.def(core.Replay(core.ReplayKeyed))))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("pay"))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = turn.Wait()
			_ = s.WaitIdle(bg)
			id := s.ID()
			pay.crash = nil
			st := restart(t, b, inner, id)
			s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")), pay.def(core.Replay(core.ReplayKeyed))))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			if rt, err := s2.RecoverTurn(bg); err != nil || rt != nil {
				t.Fatalf("RecoverTurn = %v, %v; want the call waiting for a person", rt, err)
			}
			if p := s2.Pending(); len(p) != 1 || !p[0].Recovery {
				t.Fatalf("Pending = %+v, want one recovery request", p)
			}
			if n := len(pay.ran()); n != 1 {
				t.Fatalf("pay ran %d times, want once (before the crash)", n)
			}
		})
	}
}
