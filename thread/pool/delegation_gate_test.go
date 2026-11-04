package pool_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/thread/pool"
	"github.com/weftbase/weft/thread/sqlite"
)

// A delegation is an effect like any other call (ADR 0032, ADR 0022
// §2): under a session the pool's child is created only where the
// subagent's handler would run — after the approval boundary and the
// dispatch gate. These tests hold the child to that: a parked, denied,
// fenced-off or felled delegating call never starts a child.

// gateBackend opens a durable store over a new path on each call: a
// second call is another process's empty store.
type gateBackend struct {
	name string
	open func(t *testing.T) thread.Storage
}

func gateBackends() []gateBackend {
	return []gateBackend{
		{name: "jsonl", open: func(t *testing.T) thread.Storage {
			st, err := jsonl.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
		{name: "sqlite", open: func(t *testing.T) thread.Storage {
			st, err := sqlite.Open(filepath.Join(t.TempDir(), "s.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if c, ok := st.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			})
			return st
		}},
	}
}

// childNeverStarted fails the test when a child was created or its
// model asked anything.
func childNeverStarted(t *testing.T, s *thread.Session, childModel *wefttest.Model) {
	t.Helper()
	if rs := pool.Receipts(s); len(rs) != 0 {
		t.Fatalf("receipts = %+v, want none: no child may be created", rs)
	}
	if n := len(childModel.Requests()); n != 0 {
		t.Fatalf("the child's model was asked %d times, want 0", n)
	}
}

// Under a session, a pool-wrapped tool the run must park — its own
// RequireApproval, the run's ParkOn, the agent's ParkSource — parks
// before any child exists; approved, it delegates once; denied, no
// child ever starts. Before the fix the pool delegated in middleware,
// ahead of the approval boundary: the child ran unapproved.
func TestDelegationParksBeforeTheChild(t *testing.T) {
	type rule struct {
		name    string
		wrap    []pool.WrapOption
		agent   []core.Option
		sendOpt []thread.SendOption
	}
	rules := []rule{
		{name: "RequireApproval", wrap: []pool.WrapOption{pool.ToolOptions(core.RequireApproval())}},
		{name: "ParkOn", sendOpt: []thread.SendOption{thread.RunOptions(core.ParkOn("research"))}},
		{name: "ParkSource", agent: []core.Option{core.ParkSource(func(context.Context) []string { return []string{"research"} })}},
	}
	for _, b := range gateBackends() {
		for _, r := range rules {
			for _, decide := range []string{"approve", "deny"} {
				t.Run(b.name+"/"+r.name+"/"+decide, func(t *testing.T) {
					ctx := context.Background()
					p := pool.New(2)
					defer func() { _ = p.Close(ctx) }()
					childModel := wefttest.Script(wefttest.Say("refunded"))
					child := core.New(childModel)
					opts := append([]core.Option{p.MustWrap("research", "delegates", child, r.wrap...)}, r.agent...)
					parent := core.New(wefttest.Script(
						wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "c-wrapper", Args: `{"prompt":"refund"}`}),
						wefttest.Say("all done"),
					), opts...)
					s, err := thread.Create(ctx, b.open(t), parent)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = s.Close(ctx) }()
					t1, err := s.Send(ctx, core.User("go"), r.sendOpt...)
					if err != nil {
						t.Fatal(err)
					}
					res, err := t1.Wait()
					if err != nil {
						t.Fatal(err)
					}
					if len(res.Pending) != 1 || res.Pending[0].ID != "c-wrapper" {
						t.Fatalf("pending = %+v, want the delegating call parked", res.Pending)
					}
					childNeverStarted(t, s, childModel)
					pend := s.Pending()
					if len(pend) != 1 || pend[0].CallID != "c-wrapper" || pend[0].Child != "" || pend[0].Recovery {
						t.Fatalf("Pending = %+v, want the parent's own request for c-wrapper", pend)
					}

					d := thread.Approve("c-wrapper")
					if decide == "deny" {
						d = thread.Deny("c-wrapper", "not now")
					}
					t2, err := s.Decide(ctx, bound(s, d))
					if err != nil {
						t.Fatal(err)
					}
					if t2 == nil {
						t.Fatal("Decide armed no resume")
					}
					if _, err := t2.Wait(); err != nil {
						t.Fatal(err)
					}
					if decide == "deny" {
						childNeverStarted(t, s, childModel)
						return
					}
					rs := pool.Receipts(s)
					if len(rs) != 1 || rs[0].State != pool.Done || rs[0].Call != "c-wrapper" {
						t.Fatalf("receipts = %+v, want one delegation, done", rs)
					}
					if n := len(childModel.Requests()); n != 1 {
						t.Fatalf("the child's model was asked %d times, want once", n)
					}
					// The approved delegation passed the gate: its dispatch
					// record is on file, before its acceptance receipt.
					dispatched, accepted := -1, -1
					for i, e := range s.Entries() {
						switch e := e.(type) {
						case thread.ApprovalAuditEntry:
							if e.Step == thread.StepDispatch && e.CallID == "c-wrapper" {
								dispatched = i
							}
						case thread.PoolReceiptEntry:
							if e.Status == thread.PoolAccepted && e.Call == "c-wrapper" {
								accepted = i
							}
						}
					}
					if dispatched < 0 || accepted < 0 || dispatched > accepted {
						t.Fatalf("dispatch record at %d, acceptance at %d: want the record first", dispatched, accepted)
					}
				})
			}
		}
	}
}

// fenceLost is a store whose writer has lost the session: every fence
// fails, every other call passes through.
type fenceLost struct{ thread.Storage }

func (fenceLost) Fence(context.Context, string) error { return thread.ErrLocked }

// holdFirst is an agent middleware that holds the first call it sees,
// before the approval boundary and the gate, until release.
func holdFirst(in, release chan struct{}) core.ToolMiddleware {
	var once sync.Once
	return func(next core.ToolCaller) core.ToolCaller {
		return func(ctx context.Context, call core.ToolCallPart) (string, error) {
			once.Do(func() {
				close(in)
				<-release
			})
			return next(ctx, call)
		}
	}
}

// The dispatch gate holds for a delegation: a writer whose lease is
// lost (the self-fence, WR-4) or whose turn an Interrupt felled starts
// no child. Before the fix the delegation ran in middleware, ahead of
// the gate: no dispatch record, no fence check, no felled check.
func TestDelegationPassesTheGate(t *testing.T) {
	for _, b := range gateBackends() {
		t.Run(b.name+"/lease-lost", func(t *testing.T) {
			ctx := context.Background()
			p := pool.New(2)
			defer func() { _ = p.Close(ctx) }()
			childModel := wefttest.Script(wefttest.Say("refunded"))
			parentModel := wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "c-wrapper", Args: `{"prompt":"refund"}`}),
				wefttest.Say("all done"),
			)
			s, err := thread.Create(ctx, fenceLost{b.open(t)}, core.New(parentModel,
				p.MustWrap("research", "delegates", core.New(childModel))))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(ctx) }()
			t1, err := s.Send(ctx, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := t1.Wait(); !errors.Is(err, thread.ErrLocked) {
				t.Fatalf("turn = %v, want the lease-lost cause (ErrLocked)", err)
			}
			childNeverStarted(t, s, childModel)
		})
		t.Run(b.name+"/interrupted", func(t *testing.T) {
			ctx := context.Background()
			p := pool.New(2)
			defer func() { _ = p.Close(ctx) }()
			childModel := wefttest.Script(wefttest.Say("refunded"))
			in, release := make(chan struct{}), make(chan struct{})
			s, err := thread.Create(ctx, b.open(t), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "c-wrapper", Args: `{"prompt":"refund"}`}),
				wefttest.Say("the follow-up"),
			), p.MustWrap("research", "delegates", core.New(childModel)), core.WrapTools(holdFirst(in, release))))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(ctx) }()
			t1, err := s.Send(ctx, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			<-in // the delegating call waits before the gate
			t2, err := s.Send(ctx, core.User("never mind"), thread.As(thread.Interrupt))
			if err != nil {
				t.Fatal(err)
			}
			close(release)
			_, _ = t1.Wait()
			if _, err := t2.Wait(); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("the follow-up = %v", err)
			}
			childNeverStarted(t, s, childModel)
		})
	}
}

// blockAccept is a store that holds the acceptance receipt's append
// until release, signalling held once it is held: the process dies
// there, the delegation's dispatch record on file and no receipt.
type blockAccept struct {
	thread.Storage
	held    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockAccept) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	for _, e := range entries {
		if pr, ok := e.(thread.PoolReceiptEntry); ok && pr.Status == thread.PoolAccepted {
			b.once.Do(func() { close(b.held) })
			<-b.release
			return errors.New("the process died")
		}
	}
	return b.Storage.Append(ctx, session, entries...)
}

// A crash after the delegation's dispatch record and before its
// acceptance receipt: the call may have delegated (its record says it
// passed the gate), so recovery parks it for a person as a recovery
// request — never "cut off before it started", never delegated again
// on the session's own say. Approved, it delegates once.
func TestDelegationCutOffBeforeAcceptance(t *testing.T) {
	for _, b := range gateBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			p := pool.New(2)
			childModel := wefttest.Script(wefttest.Say("refunded"))
			// The parent's model: the first process's delegates; the next
			// process's only answers once the recovered call is done.
			wrapped := func(p *pool.Pool, m *wefttest.Model, turns ...wefttest.Turn) *core.Agent {
				return core.New(wefttest.Script(turns...), p.MustWrap("research", "delegates", core.New(m)))
			}
			st := b.open(t)
			blk := &blockAccept{Storage: st, held: make(chan struct{}), release: make(chan struct{})}
			s, err := thread.Create(ctx, blk, wrapped(p, childModel,
				wefttest.ToolCalls(wefttest.Call{Name: "research", ID: "c-wrapper", Args: `{"prompt":"refund"}`}),
				wefttest.Say("all done"),
			))
			if err != nil {
				t.Fatal(err)
			}
			t1, err := s.Send(ctx, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			<-blk.held
			// The process dies here: what is on file is what the next
			// one reads.
			h, entries, _, err := st.Load(ctx, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			close(blk.release)
			_, _ = t1.Wait()
			_ = s.Close(ctx)
			_ = p.Close(ctx)

			dispatched := false
			for _, e := range entries {
				if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepDispatch && a.CallID == "c-wrapper" {
					dispatched = true
				}
				if _, ok := e.(thread.PoolReceiptEntry); ok {
					t.Fatalf("a receipt is on file: %+v", e)
				}
			}
			if !dispatched {
				t.Fatal("no dispatch record for the delegating call: the gate did not record it")
			}

			// The next process: a fresh store of the same kind holding
			// exactly what landed.
			st2 := b.open(t)
			if err := st2.Create(ctx, h); err != nil {
				t.Fatal(err)
			}
			if err := st2.Append(ctx, h.ID, entries...); err != nil {
				t.Fatal(err)
			}
			p2 := pool.New(2)
			defer func() { _ = p2.Close(ctx) }()
			childModel2 := wefttest.Script(wefttest.Say("refunded"))
			s2, err := thread.Open(ctx, st2, h.ID, wrapped(p2, childModel2, wefttest.Say("all done")))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(ctx) }()
			if err := s2.Claim(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p2.Recover(ctx, s2); err != nil {
				t.Fatalf("pool Recover = %v", err)
			}
			if _, err := s2.Send(ctx, core.User("again")); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("Send before RecoverTurn = %v, want ErrRecoveryRequired", err)
			}
			rt, err := s2.RecoverTurn(ctx)
			if err != nil {
				t.Fatalf("RecoverTurn = %v", err)
			}
			if rt != nil {
				t.Fatal("RecoverTurn started a run: the delegation went again on the session's own say")
			}
			pend := s2.Pending()
			if len(pend) != 1 || pend[0].CallID != "c-wrapper" || !pend[0].Recovery ||
				strings.Contains(pend[0].Reason, "before it started") {
				t.Fatalf("Pending = %+v, want c-wrapper as a recovery request (it may have run)", pend)
			}
			if n := len(childModel.Requests()) + len(childModel2.Requests()); n != 0 {
				t.Fatalf("a child ran %d model calls before the decision", n)
			}
			t2, err := s2.Decide(ctx, bound(s2, thread.Approve("c-wrapper")))
			if err != nil {
				t.Fatal(err)
			}
			if t2 == nil {
				t.Fatal("Decide armed no resume")
			}
			if _, err := t2.Wait(); err != nil {
				t.Fatal(err)
			}
			rs := pool.Receipts(s2)
			if len(rs) != 1 || rs[0].State != pool.Done {
				t.Fatalf("receipts = %+v, want the one approved delegation, done", rs)
			}
			if n := len(childModel2.Requests()); n != 1 {
				t.Fatalf("the child's model was asked %d times, want once", n)
			}
		})
	}
}

// P1-2 (final review): under a session, a delegation dispatched from
// inside another call's handler (Agent.CallTool) is refused before any
// child exists. Its receipt, lineage and mirror would be keyed on the
// inner call id, which is not in the parent transcript: the outer call
// would be offered as an unknown outcome beside the child's parked
// request, and approving both would delegate twice. The handler reads
// ErrNestedDelegation; the outer call answers with it and nothing
// parks.
func TestNestedCallToolDelegationIsRefused(t *testing.T) {
	for _, b := range gateBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			p := pool.New(2)
			defer func() { _ = p.Close(ctx) }()
			childModel := wefttest.Script(wefttest.Say("child done"))
			research := p.MustWrap("research", "delegates", core.New(childModel))
			var parent *core.Agent
			var innerErr error
			outer := core.Tool("outer", "", func(ctx context.Context, _ struct{}) (string, error) {
				out, err := parent.CallTool(ctx, core.ToolCallPart{ID: "inner-1", Name: "research", Args: []byte(`{"prompt":"x"}`)})
				innerErr = err
				return out, err
			})
			parentModel := wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "outer", ID: "c-outer", Args: `{}`}),
				wefttest.Say("parent done"),
			)
			parent = core.New(parentModel, outer, research)
			s, err := thread.Create(ctx, b.open(t), parent)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(ctx) }()
			turn, err := s.Send(ctx, core.User("go"))
			if err != nil {
				t.Fatal(err)
			}
			res, err := turn.Wait()
			if err != nil || res.Text() != "parent done" {
				t.Fatalf("turn = %v, %v; want it to answer", res, err)
			}
			if !errors.Is(innerErr, pool.ErrNestedDelegation) {
				t.Fatalf("CallTool err = %v, want ErrNestedDelegation", innerErr)
			}
			if pend := s.Pending(); len(pend) != 0 {
				t.Fatalf("Pending = %+v, want nothing parked", pend)
			}
			childNeverStarted(t, s, childModel)
			reqs := parentModel.Requests()
			last := reqs[len(reqs)-1]
			found := false
			for _, m := range last.Messages {
				for _, part := range m.Content {
					if r, ok := part.(core.ToolResultPart); ok && r.CallID == "c-outer" &&
						strings.Contains(r.Content, pool.ErrNestedDelegation.Error()) {
						found = true
					}
				}
			}
			if !found {
				t.Errorf("the model did not read the refusal for c-outer: %+v", last.Messages)
			}
		})
	}
}
