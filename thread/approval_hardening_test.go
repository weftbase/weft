package thread_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// hardeningStores are memory and the two durable backends, each opened
// fresh per subtest.
func hardeningStores(t *testing.T) []durableBackend {
	t.Helper()
	return append([]durableBackend{{name: "memory", open: func(*testing.T) thread.Storage { return thread.Memory() }}},
		durableBackends(t)...)
}

// A1/A13: an edit revises a request entry. A dangling call no request
// entry names (Pending's synthetic "r_…" id) has none to revise — no
// run settings, schema or metadata a revision would carry — so an Edit
// of it is refused with ErrInvalidDecision, through Decide and
// DecideSigned alike, and nothing is recorded. Approving it still works.
func TestEditOfADanglingRequestIsRefused(t *testing.T) {
	for _, b := range hardeningStores(t) {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			id := "s_dangling_edit_" + b.name
			if err := st.Create(bg, thread.Header{ID: id, Created: at(0)}); err != nil {
				t.Fatal(err)
			}
			call := core.ToolCallPart{ID: "w1", Name: "wire", Args: json.RawMessage(`{"amount":900}`)}
			if err := st.Append(bg, id,
				thread.MessageEntry{ID: "e_1", Created: at(1), Message: core.User("wire"), RunID: id + "-t1"},
				thread.MessageEntry{ID: "e_2", ParentID: "e_1", Created: at(2), Message: core.Message{Role: core.RoleAssistant, Content: []core.Part{call}}},
				thread.TurnEntry{ID: "e_3", ParentID: "e_2", Created: at(3), RunID: id + "-t1", Pending: []core.ToolCallPart{call}},
			); err != nil {
				t.Fatal(err)
			}
			var wired atomic.Int32
			ring := matrixRing(t)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")),
				core.Tool("wire", "Wire.", func(_ context.Context, _ struct {
					Amount int `json:"amount"`
				}) (string, error) {
					wired.Add(1)
					return "wired", nil
				}, core.RequireApproval())), thread.WithKeyring(ring))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			r := pendingCall(t, s, "w1")
			if !strings.HasPrefix(r.ID, "r_") {
				t.Fatalf("want a synthetic request, got %+v", r)
			}
			before := len(s.Entries())
			ed := thread.Edit("w1", json.RawMessage(`{"amount":40}`)).Bind(r)
			if _, err := s.Decide(bg, ed); !errors.Is(err, thread.ErrInvalidDecision) {
				t.Fatalf("Decide(Edit) of a dangling request = %v, want ErrInvalidDecision", err)
			}
			ch, err := s.Request("w1")
			if err != nil {
				t.Fatal(err)
			}
			sd, err := ring.Sign(ch, thread.Edit("w1", json.RawMessage(`{"amount":40}`)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.DecideSigned(bg, sd); !errors.Is(err, thread.ErrInvalidDecision) {
				t.Fatalf("DecideSigned(Edit) of a dangling request = %v, want ErrInvalidDecision", err)
			}
			if got := len(s.Entries()); got != before {
				t.Fatalf("a refused edit wrote %d entries", got-before)
			}
			rt, err := s.Decide(bg, thread.Approve("w1").Bind(r))
			if err != nil || rt == nil {
				t.Fatalf("Approve = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			if wired.Load() != 1 {
				t.Fatalf("wired %d times, want once", wired.Load())
			}
		})
	}
}

// A2 (WR-26): an Approver's answer is recorded with Via "approver"
// whatever Via it wrote — as Decide records "user" — so it can never
// pose as one of the session's own paths: "recovery" skipping the
// quorum, "grant" counted as the grant's identity.
func TestApproverCannotChooseItsVia(t *testing.T) {
	for _, via := range []string{"recovery", "grant", "expiry", "signed"} {
		t.Run(via, func(t *testing.T) {
			var ran atomic.Int32
			refund := core.Tool("refund", "Refund.", func(context.Context, struct{ ID string }) (string, error) {
				ran.Add(1)
				return "refunded", nil
			}, core.RequireApproval())
			approver := func(_ context.Context, r thread.Request) (thread.Decision, bool) {
				d := thread.Approve(r.CallID)
				d.Who, d.Via = "alice", via
				return d, true
			}
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{"ID":"1"}`}),
				wefttest.Say("done"),
			), refund), thread.Quorum(2), thread.WithApprover(approver, time.Second))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, _ := s.Send(bg, core.User("refund"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			_ = s.WaitIdle(bg)
			if ran.Load() != 0 || len(s.Pending()) != 1 {
				t.Fatalf("under Quorum(2) one Approver answer (Via %q) ran the call %d time(s), pending %d", via, ran.Load(), len(s.Pending()))
			}
			for _, e := range s.Audit() {
				if d, ok := e.(thread.ApprovalDecisionEntry); ok && d.Via != "approver" {
					t.Fatalf("the Approver's decision was recorded with Via %q, want approver", d.Via)
				}
			}
			// A second, distinct approver completes the quorum.
			d := bound(s, thread.Approve("c1"))
			d.Who = "bob"
			rt, err := s.Decide(bg, d)
			if err != nil || rt == nil {
				t.Fatalf("Decide = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			if ran.Load() != 1 {
				t.Fatalf("ran %d times, want once", ran.Load())
			}
		})
	}
}

// A2, the file a build before the fix wrote: an Approver's approval
// stored with Via "recovery" beside a Quorum(2) request is read as an
// approver's — one identity of two — never the session's own retry.
func TestStoredApproverRecoveryViaKeepsTheQuorum(t *testing.T) {
	st := thread.Memory()
	const id = "s_spoofed_via"
	if err := st.Create(bg, thread.Header{ID: id, Created: at(0)}); err != nil {
		t.Fatal(err)
	}
	run := id + "-t1"
	call := core.ToolCallPart{ID: "c1", Name: "refund", Args: json.RawMessage(`{"ID":"1"}`)}
	if err := st.Append(bg, id,
		thread.MessageEntry{ID: "e_1", Created: at(1), Message: core.User("refund"), RunID: run},
		thread.MessageEntry{ID: "e_2", ParentID: "e_1", Created: at(2), Message: core.Message{Role: core.RoleAssistant, Content: []core.Part{call}}},
		thread.ApprovalRequestEntry{ID: "e_3", ParentID: "e_2", Created: at(3), CallID: "c1", Tool: "refund", Args: call.Args,
			ArgsSHA256: sha(`{"ID":"1"}`), RunID: run, Reason: "tool requires approval", Quorum: 2},
		thread.ApprovalAuditEntry{ID: "e_4", ParentID: "e_3", Created: at(4), CallID: "c1", Step: thread.StepApprover, Outcome: "approved", RunID: run},
		thread.ApprovalDecisionEntry{ID: "e_5", ParentID: "e_4", Created: at(5), CallID: "c1", Outcome: thread.OutcomeApprove, Who: "alice", Via: "recovery", RunID: run},
		thread.TurnEntry{ID: "e_6", ParentID: "e_5", Created: at(6), RunID: run, Pending: []core.ToolCallPart{call}},
	); err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")),
		core.Tool("refund", "Refund.", func(context.Context, struct{ ID string }) (string, error) {
			ran.Add(1)
			return "refunded", nil
		}, core.RequireApproval())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	if p := s.Pending(); len(p) != 1 || p[0].Quorum != 2 {
		t.Fatalf("Pending = %+v, want the call still one approval short", p)
	}
	if _, err := s.Continue(bg); err == nil {
		_ = s.WaitIdle(bg)
	}
	if ran.Load() != 0 {
		t.Fatalf("a stored Via \"recovery\" approval ran the call alone under Quorum(2)")
	}
}

// refundSource is a ToolSource serving one RequireApproval "refund"
// tool whose input schema is in's; ran counts its runs.
func refundSource[In any](ran *atomic.Int32) core.Option {
	return core.ToolSource(func() []*core.ToolDef {
		return []*core.ToolDef{core.Tool("refund", "Refund.", func(context.Context, In) (string, error) {
			ran.Add(1)
			return "refunded", nil
		}, core.RequireApproval())}
	})
}

type refundV1 struct {
	ID string `json:"id"`
}

type refundV2 struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// A3/A4 (WR-16): a call to a ToolSource tool parks with its tool's own
// reason and its schema recorded, like a static tool's: approved after
// the source changed the tool's input it is denied with the drift text;
// after the source dropped the tool, denied as no longer available;
// unchanged, it runs. Both durable backends, across a reopen.
func TestToolSourceApprovalChecksDrift(t *testing.T) {
	cases := []struct {
		name   string
		resume func(*atomic.Int32) []core.Option
		ran    int32
		result string
	}{
		{"unchanged", func(r *atomic.Int32) []core.Option { return []core.Option{refundSource[refundV1](r)} }, 1, "refunded"},
		{"schema changed", func(r *atomic.Int32) []core.Option { return []core.Option{refundSource[refundV2](r)} }, 0,
			`DENIED: tool "refund" changed its input since this call was approved; the call was not run — call it again if it is still wanted`},
		{"tool gone", func(*atomic.Int32) []core.Option {
			return []core.Option{core.ToolSource(func() []*core.ToolDef { return nil })}
		}, 0, `DENIED: tool "refund" is no longer available; the approved call was not run`},
	}
	for _, b := range durableBackends(t) {
		for _, tc := range cases {
			t.Run(b.name+"/"+tc.name, func(t *testing.T) {
				var ran atomic.Int32
				st := b.open(t)
				s, err := thread.Create(bg, st, core.New(wefttest.Script(
					wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{"id":"1"}`}),
				), refundSource[refundV1](&ran)), thread.AutoResume(false))
				if err != nil {
					t.Fatal(err)
				}
				turn, _ := s.Send(bg, core.User("refund"))
				if _, err := turn.Wait(); err != nil {
					t.Fatal(err)
				}
				if r := pendingCall(t, s, "c1"); r.Reason != "tool requires approval" {
					t.Fatalf("a RequireApproval ToolSource tool parked with Reason %q", r.Reason)
				}
				for _, e := range s.Entries() {
					if re, ok := e.(thread.ApprovalRequestEntry); ok && re.SchemaSHA256 == "" {
						t.Fatal("the request recorded no schema for a ToolSource tool")
					}
				}
				id := s.ID()
				st = restart(t, b, st, id)
				s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")), tc.resume(&ran)...), thread.AutoResume(false))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s2.Close(bg) }()
				if _, err := s2.Decide(bg, bound(s2, thread.Approve("c1"))); err != nil {
					t.Fatal(err)
				}
				rt, err := s2.Resume(bg)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rt.Wait(); err != nil {
					t.Fatal(err)
				}
				if ran.Load() != tc.ran {
					t.Fatalf("the approved call ran %d times, want %d", ran.Load(), tc.ran)
				}
				if got := resultFor(s2, "c1"); got != tc.result {
					t.Fatalf("result %q, want %q", got, tc.result)
				}
			})
		}
	}
}

// A3b: the recovery ladder resolves a ToolSource tool like a static
// one — a cut-off ReplaySafe call served by a source is redone, not
// parked as a ReplayNever call of unknown outcome.
func TestRecoverResolvesAToolSourceToolsClass(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			look := &sideEffect{name: "look"}
			src := func() core.Option {
				return core.ToolSource(func() []*core.ToolDef { return []*core.ToolDef{look.def(core.Replay(core.ReplaySafe))} })
			}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				look.crash = cs
				return core.New(wefttest.Script(charged("look")), src())
			}, nil)
			look.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "look", ID: "call_2", Args: `{"order_id":"1"}`}),
				wefttest.Say("looked up"),
			), src()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			rt, err := s.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("RecoverTurn = %v, %v (pending %+v); want the step of safe calls redone", rt, err, s.Pending())
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A5 (R7, R14): after a Branch back to a decided boundary, the same
// decision forwarded again — unsigned or signed — is applied: it
// records nothing and runs nothing. A deliberate new decision differs
// (here another Who) and runs the call.
func TestReforwardAfterBranchIsApplied(t *testing.T) {
	for _, signed := range []bool{false, true} {
		name := map[bool]string{false: "unsigned", true: "signed"}[signed]
		t.Run(name, func(t *testing.T) {
			ring := matrixRing(t)
			pay := &payTool{}
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":9}`}),
				wefttest.Say("done"), wefttest.Say("done again"),
			), pay.def()), thread.WithKeyring(ring))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, _ := s.Send(bg, core.User("pay"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			r := pendingCall(t, s, "c1")
			d := thread.Approve("c1").Bind(r)
			d.Who = "ops"
			var sd thread.SignedDecision
			if signed {
				ch, err := s.Request("c1")
				if err != nil {
					t.Fatal(err)
				}
				if sd, err = ring.Sign(ch, d); err != nil {
					t.Fatal(err)
				}
			}
			decide := func() (*thread.Turn, error) {
				if signed {
					return s.DecideSigned(bg, sd)
				}
				return s.Decide(bg, d)
			}
			rt, err := decide()
			if err != nil || rt == nil {
				t.Fatalf("decide = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			var decID string
			for _, e := range s.Entries() {
				if de, ok := e.(thread.ApprovalDecisionEntry); ok && de.CallID == "c1" {
					decID = de.ID
				}
			}
			if err := s.Branch(bg, decID); err != nil {
				t.Fatal(err)
			}
			if len(s.Pending()) != 1 {
				t.Fatalf("pending after the Branch: %+v", s.Pending())
			}
			before := len(s.Entries())
			if rt, err := decide(); err != nil || rt != nil {
				t.Fatalf("the re-forward = %v, %v; want applied (nil, nil)", rt, err)
			}
			if got := len(s.Entries()); got != before {
				t.Fatalf("the re-forward wrote %d entries", got-before)
			}
			if args, _ := pay.ran(); len(args) != 1 {
				t.Fatalf("pay ran %d times after the re-forward, want once", len(args))
			}
			again := thread.Approve("c1").Bind(pendingCall(t, s, "c1"))
			again.Who = "ops-2"
			rt, err = s.Decide(bg, again)
			if err != nil || rt == nil {
				t.Fatalf("a new decision = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			if args, _ := pay.ran(); len(args) != 2 {
				t.Fatalf("pay ran %d times after a deliberate new decision, want twice", len(args))
			}
		})
	}
}

// A6: an edit's revision is a new request; when it stays pending (a
// quorum still to meet) OnRequest fires for it, after Decide or
// DecideSigned has recorded it — the second approver is told.
func TestEditRevisionFiresOnRequest(t *testing.T) {
	for _, signed := range []bool{false, true} {
		name := map[bool]string{false: "decide", true: "signed"}[signed]
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var fired []thread.Request
			ring := matrixRing(t)
			pay := &payTool{}
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":900}`}),
				wefttest.Say("done"),
			), pay.def()), thread.Quorum(2), thread.WithKeyring(ring), thread.OnRequest(func(r thread.Request) {
				mu.Lock()
				fired = append(fired, r)
				mu.Unlock()
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, _ := s.Send(bg, core.User("pay"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			old := pendingCall(t, s, "c1")
			ed := thread.Edit("c1", json.RawMessage(`{"to":"bob","amount":40}`)).Bind(old)
			ed.Who = "bob"
			if signed {
				ch, err := s.Request("c1")
				if err != nil {
					t.Fatal(err)
				}
				sd, err := ring.Sign(ch, ed)
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.DecideSigned(bg, sd)
				if err != nil {
					t.Fatal(err)
				}
			} else if _, err := s.Decide(bg, ed); err != nil {
				t.Fatal(err)
			}
			rev := pendingCall(t, s, "c1")
			mu.Lock()
			defer mu.Unlock()
			if len(fired) != 2 || fired[1].ID != rev.ID || fired[1].RevisionOf != old.ID {
				t.Fatalf("OnRequest fired %+v, want the park then the revision %s", fired, rev.ID)
			}
		})
	}
}

// A6, the quiet case: without a quorum the editor's approval resolves
// the revision at once, and OnRequest has nothing new to announce.
func TestEditRevisionResolvedFiresNothing(t *testing.T) {
	var fired atomic.Int32
	pay := &payTool{}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":900}`}),
		wefttest.Say("done"),
	), pay.def()), thread.OnRequest(func(thread.Request) { fired.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	turn, _ := s.Send(bg, core.User("pay"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	rt, err := s.Decide(bg, thread.Edit("c1", json.RawMessage(`{"to":"bob","amount":40}`)).Bind(pendingCall(t, s, "c1")))
	if err != nil || rt == nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if fired.Load() != 1 {
		t.Fatalf("OnRequest fired %d times, want once (the park)", fired.Load())
	}
}

// A9 (WR-27): the approval class the deciding call's context names
// (WithApprovedBy) is recorded on the decision entry, through Decide
// and DecideSigned; a plain Decide records none.
func TestApprovedByIsRecordedOnTheDecision(t *testing.T) {
	for _, door := range []string{"plain", "decide", "signed"} {
		t.Run(door, func(t *testing.T) {
			ring := matrixRing(t)
			pay := &payTool{}
			s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":9}`}),
				wefttest.Say("done"),
			), pay.def()), thread.WithKeyring(ring), thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, _ := s.Send(bg, core.User("pay"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			ctx, want := bg, ""
			if door != "plain" {
				ctx, want = thread.WithApprovedBy(bg, "inbox"), "inbox"
			}
			d := thread.Approve("c1").Bind(pendingCall(t, s, "c1"))
			if door == "signed" {
				ch, err := s.Request("c1")
				if err != nil {
					t.Fatal(err)
				}
				sd, err := ring.Sign(ch, d)
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.DecideSigned(ctx, sd)
				if err != nil {
					t.Fatal(err)
				}
			} else if _, err := s.Decide(ctx, d); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, e := range s.Audit() {
				if de, ok := e.(thread.ApprovalDecisionEntry); ok {
					got = append(got, de.ApprovedBy)
				}
			}
			if len(got) != 1 || got[0] != want {
				t.Fatalf("decision ApprovedBy = %q, want [%q]", got, want)
			}
		})
	}
}

// A7 (WR-26): a session grant is bound to the tool's input schema at
// its mint — after the tool changed its input the grant no longer
// matches and the call parks; a grant minted without a schema (a tool
// the agent did not hold, or a grant written before the field) matches
// on name and arguments alone.
func TestGrantIsBoundToTheToolSchema(t *testing.T) {
	st := thread.Memory()
	var ran atomic.Int32
	s, err := thread.Create(bg, st, core.New(wefttest.Script(), refundSource[refundV1](&ran)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Grant(bg, thread.Grant{Tool: "refund", Who: "ops"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Grant(bg, thread.Grant{Tool: "other", Who: "ops"}); err != nil {
		t.Fatal(err)
	}
	var schemas []string
	for _, e := range s.Entries() {
		if g, ok := e.(thread.GrantEntry); ok {
			schemas = append(schemas, g.SchemaSHA256)
		}
	}
	if len(schemas) != 2 || schemas[0] == "" || schemas[1] != "" {
		t.Fatalf("grant schemas = %q, want the refund tool's hash and none for an unknown tool", schemas)
	}
	id := s.ID()
	if err := s.Close(bg); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		source core.Option
		parks  bool
	}{
		{"same schema", refundSource[refundV1](&ran), false},
		{"changed schema", refundSource[refundV2](&ran), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c_" + strings.ReplaceAll(tc.name, " ", "_"), Args: `{"id":"1"}`}),
				wefttest.Say("done"),
			), tc.source))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			turn, err := s.Send(bg, core.User("refund"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			_ = s.WaitIdle(bg)
			if parked := len(s.Pending()) == 1; parked != tc.parks {
				t.Fatalf("parked = %v, want %v (pending %+v)", parked, tc.parks, s.Pending())
			}
			if tc.parks {
				_, _ = s.Decide(bg, bound(s, thread.Deny(s.Pending()[0].CallID, "no")))
				_ = s.WaitIdle(bg)
			}
		})
	}
}

// A7 (WR-26): an approval a session grant made is re-validated when the
// resume starts — a grant revoked between the chain and the resume
// leaves the call unrun (the core's "no decision" denial), and the
// resume's audit entry names the grant.
func TestRevokedGrantApprovalDoesNotRun(t *testing.T) {
	var refunds, pays atomic.Int32
	refund := core.Tool("refund", "Refund.", func(context.Context, struct{ ID string }) (string, error) {
		refunds.Add(1)
		return "refunded", nil
	}, core.RequireApproval())
	pay := core.Tool("pay", "Pay.", func(context.Context, struct{ ID string }) (string, error) {
		pays.Add(1)
		return "paid", nil
	}, core.RequireApproval())
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(
			wefttest.Call{Name: "refund", ID: "c1", Args: `{"ID":"1"}`},
			wefttest.Call{Name: "pay", ID: "c2", Args: `{"ID":"2"}`},
		),
		wefttest.Say("done"),
	), refund, pay))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	if err := s.Grant(bg, thread.Grant{Tool: "refund", Who: "ops"}); err != nil {
		t.Fatal(err)
	}
	var grantID string
	for _, e := range s.Entries() {
		if g, ok := e.(thread.GrantEntry); ok {
			grantID = g.ID
		}
	}
	turn, _ := s.Send(bg, core.User("refund and pay"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	// The grant approved c1 in the chain; c2 waits for a person.
	if p := s.Pending(); len(p) != 1 || p[0].CallID != "c2" {
		t.Fatalf("Pending = %+v, want c2 alone", p)
	}
	if err := s.Revoke(bg, grantID); err != nil {
		t.Fatal(err)
	}
	rt, err := s.Decide(bg, bound(s, thread.Approve("c2")))
	if err != nil || rt == nil {
		t.Fatalf("Decide = %v, %v", rt, err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if refunds.Load() != 0 || pays.Load() != 1 {
		t.Fatalf("refunds %d pays %d, want the revoked grant's call unrun and the approved one run", refunds.Load(), pays.Load())
	}
	if got := resultFor(s, "c1"); got != "DENIED: no decision" {
		t.Fatalf("the revoked grant's call read %q", got)
	}
	audited := false
	for _, e := range s.Audit() {
		if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepResume && a.Outcome == "grant_revoked" && a.GrantID == grantID && a.CallID == "c1" {
			audited = true
		}
	}
	if !audited {
		t.Fatal("the resume's audit names no revoked grant")
	}
}
