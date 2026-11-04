package thread_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// crashStore stands in a process that dies: from the moment it is
// down, nothing more reaches the file — every Append fails, as no
// write lands after a SIGKILL. trip, when set, takes it down on the
// first batch it matches, before that batch is written.
type crashStore struct {
	thread.Storage
	down atomic.Bool
	trip func([]thread.Entry) bool
}

func (c *crashStore) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	if c.trip != nil && !c.down.Load() && c.trip(entries) {
		c.down.Store(true)
	}
	if c.down.Load() {
		return errors.New("the process is gone")
	}
	return c.Storage.Append(ctx, session, entries...)
}

// crashModel takes its store down when its n-th model call starts:
// a crash during a model call.
type crashModel struct {
	core.Model
	n     int
	calls atomic.Int32
	store *crashStore
}

func (m *crashModel) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	if int(m.calls.Add(1)) == m.n {
		m.store.down.Store(true)
	}
	return m.Model.Stream(ctx, req)
}

// sideEffect is a tool that records each execution's idempotency key;
// crash, when set, takes the store down inside the handler — the
// process dies while the call runs.
type sideEffect struct {
	name  string
	mu    sync.Mutex
	keys  []string
	crash *crashStore
}

func (c *sideEffect) def(opts ...core.ToolOption) *core.ToolDef {
	name := c.name
	if name == "" {
		name = "charge"
	}
	return core.Tool(name, "A side effect.", func(ctx context.Context, in refundInput) (string, error) {
		call, _ := core.CallFromContext(ctx)
		c.mu.Lock()
		c.keys = append(c.keys, call.IdempotencyKey)
		cr := c.crash
		c.mu.Unlock()
		if cr != nil {
			cr.down.Store(true)
		}
		return name + " " + in.OrderID, nil
	}, opts...)
}

func (c *sideEffect) ran() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.keys...)
}

// crashTurn runs one turn on a fresh session over b, with the crash
// the caller arranged, and returns the session id and the store to
// restart.
func crashTurn(t *testing.T, b durableBackend, agent func(*crashStore) *core.Agent, setup func(*crashStore), opts ...thread.SessionOption) (string, thread.Storage) {
	t.Helper()
	inner := b.open(t)
	cs := &crashStore{Storage: inner}
	if setup != nil {
		setup(cs)
	}
	s, err := thread.Create(bg, cs, agent(cs), opts...)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(bg, core.User("charge it"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait() // the turn's end never reaches the file
	if !cs.down.Load() {
		t.Fatal("the crash never happened")
	}
	_ = s.WaitIdle(bg)
	return s.ID(), inner
}

// charged is a one-call script: the model asks for call_1.
func charged(name string) wefttest.Turn {
	return wefttest.ToolCalls(wefttest.Call{Name: name, ID: "call_1", Args: `{"order_id":"1"}`})
}

// Rule B and the refusal: a crash while a ReplayNever call runs leaves
// it without a result. After a restart, Send and Continue refuse
// (ErrRecoveryRequired) — a new turn would answer the call with a
// synthetic result and the model could ask for it again. RecoverTurn
// parks it as a recovery request, outcome unknown; approving it — the
// person's "retry" — runs it once more with the same key. Both file
// backends, a fresh store for the reopen.
func TestRecoverParksAnUnknownOutcome(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				charge.crash = cs
				return core.New(wefttest.Script(charged("charge")), charge.def())
			}, nil)
			charge.crash = nil
			st = restart(t, b, st, id)

			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("charged once")), charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Send(bg, core.User("hello?")); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("Send after the crash = %v, want ErrRecoveryRequired", err)
			}
			if _, err := s.Continue(bg); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("Continue after the crash = %v, want ErrRecoveryRequired", err)
			}
			rt, err := s.RecoverTurn(bg)
			if err != nil || rt != nil {
				t.Fatalf("RecoverTurn = %v, %v; want the call parked", rt, err)
			}
			p := s.Pending()
			if len(p) != 1 || !p[0].Recovery || p[0].CallID != "call_1" || p[0].RunID != id+"-t1" ||
				!strings.Contains(p[0].Reason, "may have run") || p[0].IdempotencyKey != charge.ran()[0] {
				t.Fatalf("Pending after RecoverTurn = %+v (first key %v)", p, charge.ran())
			}
			if _, err := s.RecoverTurn(bg); !errors.Is(err, thread.ErrNotPending) {
				t.Fatalf("a second RecoverTurn = %v, want ErrNotPending", err)
			}
			if len(charge.ran()) != 1 {
				t.Fatalf("the cut-off call ran again unasked: %v", charge.ran())
			}
			if _, err := s.Decide(bg, bound(s, thread.ApproveAlways("call_1"))); !errors.Is(err, thread.ErrInvalidDecision) {
				t.Fatalf("ApproveAlways on a recovery request = %v, want ErrInvalidDecision", err)
			}
			rt, err = s.Decide(bg, bound(s, thread.Approve("call_1")))
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			res, err := rt.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if keys := charge.ran(); len(keys) != 2 || keys[0] != keys[1] {
				t.Fatalf("retry keys = %v, want the same key twice", keys)
			}
			if res.Text() != "charged once" {
				t.Fatalf("resume answered %q", res.Text())
			}
			var te thread.TurnEntry
			for _, e := range s.Entries() {
				if x, ok := e.(thread.TurnEntry); ok && x.RunID == id+"-t1" {
					te = x
				}
			}
			if !te.CutOff || te.Canceled || len(te.Pending) != 1 {
				t.Fatalf("the cut-off run's turn entry = %+v", te)
			}
			if err := s.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Rules C and E over a mixed step: a ReplaySafe, a ReplayKeyed and a
// ReplayNever call cut off together, all three dispatched. RecoverTurn
// approves the keyed call itself — Via "recovery", beside a "retry"
// audit step — answers the safe one "not run again" (a park rule may
// have held it: the model asks again if it still needs it), and parks
// the never call for a person; the person's approval runs the keyed
// and the never call under their first keys. Both file backends.
func TestRecoverMixedReplayClasses(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			read := &sideEffect{name: "read"}
			pay := &sideEffect{name: "pay"}
			mail := &sideEffect{name: "mail"}
			tools := func() []core.Option {
				return []core.Option{read.def(core.Replay(core.ReplaySafe)), pay.def(core.Replay(core.ReplayKeyed)), mail.def(core.Sequential())}
			}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				mail.crash = cs // the last call: the others ran first
				return core.New(wefttest.Script(wefttest.ToolCalls(
					wefttest.Call{Name: "read", ID: "c_read", Args: `{"order_id":"1"}`},
					wefttest.Call{Name: "pay", ID: "c_pay", Args: `{"order_id":"1"}`},
					wefttest.Call{Name: "mail", ID: "c_mail", Args: `{"order_id":"1"}`},
				)), tools()...)
			}, nil)
			mail.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("all done")), tools()...))
			if err != nil {
				t.Fatal(err)
			}
			if rt, err := s.RecoverTurn(bg); err != nil || rt != nil {
				t.Fatalf("RecoverTurn = %v, %v; want the never call parked", rt, err)
			}
			p := s.Pending()
			if len(p) != 1 || p[0].CallID != "c_mail" || !p[0].Recovery {
				t.Fatalf("Pending = %+v, want only the ReplayNever call", p)
			}
			retries, recoveryApprovals, notRerun := 0, 0, 0
			for _, e := range s.Audit() {
				switch e := e.(type) {
				case thread.ApprovalAuditEntry:
					if e.Step == thread.StepRecover && e.Outcome == "retry" {
						retries++
					}
				case thread.ApprovalDecisionEntry:
					if e.Via == "recovery" && e.Outcome == thread.OutcomeApprove && e.RequestID != "" {
						recoveryApprovals++
					}
					if e.Via == "recovery" && e.Outcome == thread.OutcomeResolveError && e.CallID == "c_read" {
						notRerun++
					}
				}
			}
			if retries != 1 || recoveryApprovals != 1 || notRerun != 1 {
				t.Fatalf("audit: %d retry steps, %d recovery approvals, %d safe calls not re-run; want 1, 1 and 1", retries, recoveryApprovals, notRerun)
			}
			rt, err := s.Decide(bg, bound(s, thread.Approve("c_mail")))
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			if res, err := rt.Wait(); err != nil || res.Text() != "all done" {
				t.Fatalf("resume = %v, %v", res, err)
			}
			for _, tool := range []*sideEffect{pay, mail} {
				if keys := tool.ran(); len(keys) != 2 || keys[0] != keys[1] {
					t.Fatalf("%s keys = %v, want one key twice", tool.name, keys)
				}
			}
			if keys := read.ran(); len(keys) != 1 {
				t.Fatalf("read ran %d times, want once: a cut-off ReplaySafe call is not run again", len(keys))
			}
			_ = s.Close(bg)
		})
	}
}

// A ReplayKeyed call alone is retried at once: RecoverTurn returns the
// resume, which runs it again under its first key.
func TestRecoverRetriesAKeyedCall(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				charge.crash = cs
				return core.New(wefttest.Script(charged("charge")), charge.def(core.Replay(core.ReplayKeyed)))
			}, nil, thread.Quorum(2)) // the session's own retry needs no quorum
			charge.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")), charge.def(core.Replay(core.ReplayKeyed))), thread.Quorum(2))
			if err != nil {
				t.Fatal(err)
			}
			rt, err := s.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("RecoverTurn = %v, %v; want the retry running", rt, err)
			}
			res, err := rt.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if keys := charge.ran(); len(keys) != 2 || keys[0] != keys[1] || res.Text() != "done" {
				t.Fatalf("keys %v, answer %q", keys, res.Text())
			}
			if err := s.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A crash during a model call: the finished step is on file and no
// call is left. RecoverTurn closes the cut-off run and continues from
// the last finished step under a fresh run id, with no new prompt.
func TestRecoverContinuesAfterAModelCrash(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				m := &crashModel{Model: wefttest.Script(charged("charge"), wefttest.Say("never")), n: 2, store: cs}
				return core.New(m, charge.def())
			}, nil)
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("charged, and done")), charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			rt, err := s.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("RecoverTurn = %v, %v", rt, err)
			}
			if rt.RunID() != id+"-t2" {
				t.Fatalf("continuation run id = %s", rt.RunID())
			}
			res, err := rt.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if res.Text() != "charged, and done" || len(charge.ran()) != 1 {
				t.Fatalf("answer %q, charges %v", res.Text(), charge.ran())
			}
			users := 0
			for _, m := range s.Context() {
				if m.Role == core.RoleUser {
					users++
				}
			}
			if users != 1 {
				t.Fatalf("the continuation added a prompt: %d user messages", users)
			}
			if err := s.Close(bg); err != nil {
				t.Fatal(err)
			}
			s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("third"))))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s2.RecoverTurn(bg); !errors.Is(err, thread.ErrNotPending) {
				t.Fatalf("RecoverTurn after a clean continuation = %v", err)
			}
			t3, err := s2.Send(bg, core.User("more"))
			if err != nil {
				t.Fatal(err)
			}
			if t3.RunID() != id+"-t3" {
				t.Fatalf("next run id = %s", t3.RunID())
			}
			_, _ = t3.Wait()
			_ = s2.Close(bg)
		})
	}
}

// A crash while only ReplaySafe calls run: RecoverTurn drops the
// unfinished step and asks the model again; Send was never refused.
func TestRecoverRedoesAStepOfSafeCalls(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			look := &sideEffect{name: "look"}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				look.crash = cs
				return core.New(wefttest.Script(charged("look")), look.def(core.Replay(core.ReplaySafe)))
			}, nil)
			look.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "look", ID: "call_2", Args: `{"order_id":"1"}`}),
				wefttest.Say("looked up"),
			), look.def(core.Replay(core.ReplaySafe))))
			if err != nil {
				t.Fatal(err)
			}
			rt, err := s.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("RecoverTurn = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			for _, m := range s.Context() {
				for _, p := range m.Content {
					if c, ok := p.(core.ToolCallPart); ok && c.ID == "call_1" {
						t.Fatal("the unfinished step stayed on the path")
					}
				}
			}
			if err := s.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A turn that answered and lost only its end: RecoverTurn writes the
// end and runs nothing.
func TestRecoverClosesAnAnsweredTurn(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			id, st := crashTurn(t, b, func(*crashStore) *core.Agent {
				return core.New(wefttest.Script(wefttest.Say("the answer")))
			}, func(cs *crashStore) {
				cs.trip = func(es []thread.Entry) bool {
					for _, e := range es {
						if _, ok := e.(thread.TurnEntry); ok {
							return true
						}
					}
					return false
				}
			})
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script()))
			if err != nil {
				t.Fatal(err)
			}
			if rt, err := s.RecoverTurn(bg); err != nil || rt != nil {
				t.Fatalf("RecoverTurn = %v, %v", rt, err)
			}
			if _, err := s.RecoverTurn(bg); !errors.Is(err, thread.ErrNotPending) {
				t.Fatalf("second RecoverTurn = %v", err)
			}
			_ = s.Close(bg)
		})
	}
}

// A tool whose own RequireApproval parks it never started: cut off in
// a step beside a call of unknown outcome, it parks as an ordinary
// request, not a recovery one.
func TestRecoverParksAGatedCallAsAnOrdinaryRequest(t *testing.T) {
	b := durableBackends(t)[0]
	charge := &sideEffect{}
	gate := &sideEffect{name: "wire"}
	tools := func() []core.Option { return []core.Option{charge.def(), gate.def(core.RequireApproval())} }
	id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
		charge.crash = cs
		return core.New(wefttest.Script(wefttest.ToolCalls(
			wefttest.Call{Name: "charge", ID: "c1", Args: `{"order_id":"1"}`},
			wefttest.Call{Name: "wire", ID: "c2", Args: `{"order_id":"2"}`},
		)), tools()...)
	}, nil)
	charge.crash = nil
	st = restart(t, b, st, id)
	s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("ok")), tools()...))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	if _, err := s.RecoverTurn(bg); err != nil {
		t.Fatal(err)
	}
	p := s.Pending()
	if len(p) != 2 || !p[0].Recovery || p[1].Recovery || p[1].Reason != "tool requires approval" {
		t.Fatalf("Pending = %+v", p)
	}
	if len(gate.ran()) != 0 {
		t.Fatal("the gated call ran")
	}
}

// toolResult is the content the run gave callID's last result.
func toolResult(res *core.RunResult, callID string) string {
	out := ""
	for _, m := range res.Messages {
		for _, p := range m.Content {
			if r, ok := p.(core.ToolResultPart); ok && r.CallID == callID {
				out = r.Content
			}
		}
	}
	return out
}

// failWhen refuses every Append batch its predicate matches, and lets
// every other through: one kind of write fails, the rest land.
type failWhen struct {
	thread.Storage
	when func([]thread.Entry) bool
}

func (f *failWhen) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	if f.when(entries) {
		return errors.New("this write is refused")
	}
	return f.Storage.Append(ctx, session, entries...)
}

func assistantStep(es []thread.Entry) bool {
	for _, e := range es {
		if me, ok := e.(thread.MessageEntry); ok && me.Message.Role == core.RoleAssistant {
			return true
		}
	}
	return false
}

// Rule A: a step whose assistant message cannot be written is retried
// before its call that is not ReplaySafe runs; when the retry fails
// too, the call fails closed — the handler never runs and the model
// reads an error wrapping core.ErrNotDispatched — while a ReplaySafe
// call in the same step runs. Both backends.
func TestFailedStepAppendFailsClosed(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			look := &sideEffect{name: "look"}
			st := &failWhen{Storage: b.open(t), when: assistantStep}
			s, err := thread.Create(bg, st, core.New(wefttest.Script(
				wefttest.ToolCalls(
					wefttest.Call{Name: "charge", ID: "c1", Args: `{"order_id":"1"}`},
					wefttest.Call{Name: "look", ID: "c2", Args: `{"order_id":"1"}`},
				),
				wefttest.Say("could not charge"),
			), charge.def(), look.def(core.Replay(core.ReplaySafe))))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("charge it"))
			if err != nil {
				t.Fatal(err)
			}
			res, _ := turn.Wait()
			if n := len(charge.ran()); n != 0 {
				t.Fatalf("the handler ran %d times with its step not durable", n)
			}
			if len(look.ran()) != 1 {
				t.Fatalf("the ReplaySafe sibling ran %d times, want once", len(look.ran()))
			}
			if res != nil {
				if r := toolResult(res, "c1"); !strings.Contains(r, core.ErrNotDispatched.Error()) || !strings.Contains(r, "not durable") {
					t.Fatalf("the model saw %q", r)
				}
			}
			_ = s.Close(bg)
		})
	}
}

// A provider that reuses call ids across steps (call_1 twice): each
// execution's key is its own step's, and when the second step's
// message is not durable the call is refused — never keyed to the
// first step's entry. Both backends.
func TestRepeatedCallIDsAcrossSteps(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			model := func() core.Model {
				return wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "call_1", Args: `{"order_id":"1"}`}),
					wefttest.ToolCalls(wefttest.Call{Name: "charge", ID: "call_1", Args: `{"order_id":"2"}`}),
					wefttest.Say("done"),
				)
			}
			s, err := thread.Create(bg, b.open(t), core.New(model(), charge.def()))
			if err != nil {
				t.Fatal(err)
			}
			turn, _ := s.Send(bg, core.User("charge twice"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			keys := charge.ran()
			if len(keys) != 2 || keys[0] == keys[1] {
				t.Fatalf("keys = %v, want two distinct", keys)
			}
			_ = s.Close(bg)

			charge2 := &sideEffect{}
			assistants := 0
			st := &failWhen{Storage: b.open(t), when: func(es []thread.Entry) bool {
				if assistantStep(es) {
					assistants++
					return assistants >= 2
				}
				return false
			}}
			s2, err := thread.Create(bg, st, core.New(model(), charge2.def()))
			if err != nil {
				t.Fatal(err)
			}
			t2, _ := s2.Send(bg, core.User("charge twice"))
			_, _ = t2.Wait()
			if n := len(charge2.ran()); n != 1 {
				t.Fatalf("charges = %d, want only the first step's", n)
			}
		})
	}
}

// stallModel's first call waits for release, then answers.
type stallModel struct {
	core.Model
	release chan struct{}
	entered chan struct{}
	n       int
}

func (m *stallModel) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	m.n++
	if m.n == 1 {
		close(m.entered)
		<-m.release
	}
	return m.Model.Stream(ctx, req)
}

// A zombie worker against recovery. A runs a turn and stalls in its
// model call; B opens the session from the file, where the turn reads
// cut off. While A holds the session B's RecoverTurn is refused
// (ErrLocked): the turn is running, not cut off. Once A's hold is gone
// — a dead worker's lease, taken over — B recovers, and A's late call
// is refused by rule A: its step cannot land, so its handler never
// runs. Both backends.
func TestZombieWorkerAgainstRecovery(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			chargeA := &sideEffect{}
			m := &stallModel{Model: wefttest.Script(charged("charge"), wefttest.Say("A done")),
				release: make(chan struct{}), entered: make(chan struct{})}
			stA := b.open(t)
			a, err := thread.Create(bg, stA, core.New(m, chargeA.def()))
			if err != nil {
				t.Fatal(err)
			}
			turnA, err := a.Send(bg, core.User("charge it"))
			if err != nil {
				t.Fatal(err)
			}
			<-m.entered

			chargeB := &sideEffect{}
			stB := b.open(t)
			bs, err := thread.Open(bg, stB, a.ID(), core.New(wefttest.Script(wefttest.Say("B continues")), chargeB.def()))
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Claim(bg); !errors.Is(err, thread.ErrLocked) {
				t.Fatalf("B's Claim while A holds the session = %v, want ErrLocked", err)
			}
			if _, err := bs.RecoverTurn(bg); !errors.Is(err, thread.ErrLocked) {
				t.Fatalf("B's RecoverTurn while A holds the session = %v, want ErrLocked", err)
			}
			abandon(t, stA, a.ID()) // A's hold lapses: a dead worker's, taken over
			bs2, err := thread.Open(bg, stB, a.ID(), core.New(wefttest.Script(wefttest.Say("B continues")), chargeB.def()))
			if err != nil {
				t.Fatal(err)
			}
			if err := bs2.Claim(bg); err != nil {
				t.Fatalf("B's Claim after A's lease lapsed = %v", err)
			}
			rt, err := bs2.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("B's RecoverTurn = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			close(m.release) // the zombie wakes up and asks for its charge
			_, _ = turnA.Wait()
			if n := len(chargeA.ran()); n != 0 {
				t.Fatalf("the zombie's handler ran %d times", n)
			}
			_ = bs2.Close(bg)
		})
	}
}

// WR-6 under a session: a call with an effect whose handler outlives
// its Timeout has an unknown outcome. The session does not hand the
// model a guess: a ReplayNever call parks as a recovery request (the
// turn ends at that boundary, the step's other results kept), and a
// ReplayKeyed one is retried at once under its same key. Both
// backends, the recovery request across a restart.
func TestTimeoutParksForRecovery(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			keys := map[string][]string{}
			slow := func(name string, opts ...core.ToolOption) *core.ToolDef {
				return core.Tool(name, "Slow.", func(ctx context.Context, _ struct{}) (string, error) {
					c, _ := core.CallFromContext(ctx)
					mu.Lock()
					keys[name] = append(keys[name], c.IdempotencyKey)
					n := len(keys[name])
					mu.Unlock()
					if n == 1 {
						<-ctx.Done() // the first attempt never answers in time
						return "", ctx.Err()
					}
					return name + " ok", nil
				}, append(opts, core.Timeout(20*time.Millisecond))...)
			}
			tools := func() []core.Option {
				return []core.Option{slow("pay"), slow("ship", core.Replay(core.ReplayKeyed)),
					core.Tool("read", "Read.", func(context.Context, struct{}) (string, error) { return "read ok", nil }, core.Replay(core.ReplaySafe))}
			}
			st := b.open(t)
			s, err := thread.Create(bg, st, core.New(wefttest.Script(
				wefttest.ToolCalls(
					wefttest.Call{Name: "read", ID: "r", Args: `{}`},
					wefttest.Call{Name: "pay", ID: "p", Args: `{}`},
					wefttest.Call{Name: "ship", ID: "s", Args: `{}`},
				),
			), tools()...))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			res, err := turn.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if toolResult(res, "r") != "read ok" {
				t.Fatalf("the safe call's result was not kept: %+v", res.Messages)
			}
			p := s.Pending()
			if len(p) != 1 || p[0].CallID != "p" || !p[0].Recovery {
				t.Fatalf("Pending = %+v, want the payment as a recovery request", p)
			}
			id := s.ID()
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")), tools()...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			rt, err := s2.Decide(bg, bound(s2, thread.Approve("p")))
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			if res, err := rt.Wait(); err != nil || res.Text() != "done" {
				t.Fatalf("resume = %v, %v", res, err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, name := range []string{"pay", "ship"} {
				if k := keys[name]; len(k) != 2 || k[0] != k[1] {
					t.Fatalf("%s keys = %v, want one key twice", name, k)
				}
			}
		})
	}
}

// WR-5: a turn whose step ran a call with effects and then overflowed
// the context is not re-run from its start — the re-run would not see
// the effect and could repeat it. The turn ends with the overflow, the
// call ran once, and its result stays on the line. Both backends.
func TestOverflowNeverRepeatsEffects(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &sideEffect{name: "pay"}
			s, err := thread.Create(bg, b.open(t), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "p1", Args: `{"order_id":"1"}`}),
				wefttest.Fail(core.ErrContextOverflow),
				wefttest.Say("a summary"),
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "p2", Args: `{"order_id":"1"}`}), // a re-run would pay again
				wefttest.Say("re-run answer"),
			), pay.def()))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("pay"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); !errors.Is(err, core.ErrContextOverflow) {
				t.Fatalf("turn = %v, want the overflow", err)
			}
			if n := len(pay.ran()); n != 1 {
				t.Fatalf("pay ran %d times, want once", n)
			}
			found := false
			for _, m := range s.Context() {
				for _, p := range m.Content {
					if r, ok := p.(core.ToolResultPart); ok && r.CallID == "p1" && r.Content == "pay 1" {
						found = true
					}
				}
			}
			if !found {
				t.Fatal("the call's result left the line")
			}
			_ = s.Close(bg)
		})
	}
}

// A Rollback never hides an effect: a running turn that already ran a
// call with effects is not rolled back off the line. The Send is
// accepted as an Interrupt instead and says so (ErrRollbackRefused,
// beside its Turn): the follow-up runs, and the paid call and its
// result stay in the context the model reads. Both backends.
func TestRollbackPastAnEffectIsRefused(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &sideEffect{name: "pay"}
			m := &hangOnce{Model: wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "p1", Args: `{"order_id":"1"}`}),
				wefttest.Say("the follow-up"),
			), inCall: make(chan struct{})}
			s, err := thread.Create(bg, b.open(t), core.New(m, pay.def()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			t1, err := s.Send(bg, core.User("pay"))
			if err != nil {
				t.Fatal(err)
			}
			<-m.inCall // the payment ran; the second model call hangs
			t2, err := s.Send(bg, core.User("never mind"), thread.As(thread.Rollback))
			if !errors.Is(err, thread.ErrRollbackRefused) || t2 == nil {
				t.Fatalf("Send under Rollback = %v, %v; want its Turn and ErrRollbackRefused", t2, err)
			}
			if _, err := t1.Wait(); !errors.Is(err, context.Canceled) {
				t.Fatalf("the felled turn = %v", err)
			}
			if _, err := t2.Wait(); err != nil {
				t.Fatalf("the follow-up = %v", err)
			}
			found := false
			for _, msg := range s.Context() {
				for _, p := range msg.Content {
					if r, ok := p.(core.ToolResultPart); ok && r.CallID == "p1" && r.Content == "pay 1" {
						found = true
					}
				}
			}
			if !found || len(pay.ran()) != 1 {
				t.Fatalf("the payment left the line (found %v) or ran %d times", found, len(pay.ran()))
			}
		})
	}
}

// hangOnce's second call blocks until its context ends; every other
// call goes to the script.
type hangOnce struct {
	core.Model
	calls  atomic.Int32
	inCall chan struct{}
}

func (m *hangOnce) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	if m.calls.Add(1) != 2 {
		return m.Model.Stream(ctx, req)
	}
	return func(yield func(core.ModelEvent, error) bool) {
		close(m.inCall)
		<-ctx.Done()
		yield(nil, ctx.Err())
	}
}
