package thread_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// payTool records the arguments and the key of each execution.
type payTool struct {
	mu   sync.Mutex
	args []string
	keys []string
	// crash, when set, takes the store down inside the handler.
	crash *crashStore
}

func (p *payTool) def() *core.ToolDef {
	return core.Tool("pay", "Pay.", func(ctx context.Context, in struct {
		To     string `json:"to"`
		Amount int    `json:"amount"`
	}) (string, error) {
		c, _ := core.CallFromContext(ctx)
		p.mu.Lock()
		p.args = append(p.args, fmt.Sprintf(`{"to":%q,"amount":%d}`, in.To, in.Amount))
		p.keys = append(p.keys, c.IdempotencyKey)
		cr := p.crash
		p.mu.Unlock()
		if cr != nil {
			cr.down.Store(true)
		}
		return "paid", nil
	}, core.RequireApproval())
}

func (p *payTool) ran() ([]string, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.args...), append([]string(nil), p.keys...)
}

func sha(b string) string {
	h := sha256.Sum256([]byte(b))
	return hex.EncodeToString(h[:])
}

func pendingCall(t *testing.T, s *thread.Session, callID string) thread.Request {
	t.Helper()
	for _, r := range s.Pending() {
		if r.CallID == callID {
			return r
		}
	}
	t.Fatalf("call %s is not pending: %+v", callID, s.Pending())
	return thread.Request{}
}

// An edit is a revision (D1). It closes the request and opens a new
// one for the same call on the edited arguments, in canonical form
// (sorted keys, no space, no HTML escaping), linked to the old one; the
// editor's decision is one approval of it. Without a quorum that
// resolves it: the resume runs the call on the edited arguments, under
// a key that covers them, and the model reads the pinned EDITED line.
// The revision survives a reopen. Both backends.
func TestEditOpensARevision(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &payTool{}
			agent := func(turns ...wefttest.Turn) *core.Agent { return core.New(wefttest.Script(turns...), pay.def()) }
			st := b.open(t)
			s, err := thread.Create(bg, st, agent(wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":900}`})),
				thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			turn, _ := s.Send(bg, core.User("pay bob"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			shown := pendingCall(t, s, "c1")
			edit := thread.Edit("c1", json.RawMessage(`{ "to": "<bob>", "amount": 40 }`)).Bind(shown)
			if string(edit.EditedArgs) != `{"amount":40,"to":"<bob>"}` {
				t.Fatalf("canonical edit = %s", edit.EditedArgs)
			}
			if _, err := s.Decide(bg, edit); err != nil {
				t.Fatal(err)
			}
			if p := s.Pending(); len(p) != 0 {
				t.Fatalf("an edit under no quorum left %+v pending", p)
			}
			id := s.ID()
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, agent(wefttest.Say("done")), thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			var rev thread.ApprovalRequestEntry
			var editEntry thread.ApprovalDecisionEntry
			for _, e := range s2.Entries() {
				switch e := e.(type) {
				case thread.ApprovalRequestEntry:
					if e.RevisionOf != "" {
						rev = e
					}
				case thread.ApprovalDecisionEntry:
					if e.Outcome == thread.OutcomeEdit {
						editEntry = e
					}
				}
			}
			canonical := `{"amount":40,"to":"<bob>"}`
			if rev.RevisionOf != shown.ID || rev.ArgsSHA256 != sha(canonical) {
				t.Fatalf("revision = %+v, want it to revise %s on the canonical edit", rev, shown.ID)
			}
			if editEntry.ArgsSHA256 != shown.ArgsSHA256 || editEntry.EditedArgsSHA256 != sha(canonical) {
				t.Fatalf("edit entry hashes = %s / %s", editEntry.ArgsSHA256, editEntry.EditedArgsSHA256)
			}
			rt, err := s2.Resume(bg)
			if err != nil {
				t.Fatal(err)
			}
			res, err := rt.Wait()
			if err != nil {
				t.Fatal(err)
			}
			args, keys := pay.ran()
			if len(args) != 1 || args[0] != `{"to":"<bob>","amount":40}` {
				t.Fatalf("pay ran with %v, want the edited arguments", args)
			}
			entry := callEntry(t, s2, "c1")
			if want := thread.IdempotencyKey(id, entry, "c1", sha(canonical)); keys[0] != want || keys[0] == thread.IdempotencyKey(id, entry, "c1", "") {
				t.Fatalf("key = %s, want the edited key %s", keys[0], want)
			}
			if r := toolResult(res, "c1"); !strings.HasPrefix(r, "EDITED: the call ran with the arguments "+canonical+"\n") {
				t.Fatalf("the model read %q", r)
			}
		})
	}
}

// Under Quorum(2) an edit does not decide alone (G1): the revision
// still needs two distinct approvers, the editor counted, and an
// approval of the old arguments does not count. Both backends, the
// revision read back after a reopen.
func TestEditUnderQuorumNeedsAnotherApprover(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &payTool{}
			agent := func(turns ...wefttest.Turn) *core.Agent { return core.New(wefttest.Script(turns...), pay.def()) }
			st := b.open(t)
			s, err := thread.Create(bg, st, agent(wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":900}`})),
				thread.Quorum(2))
			if err != nil {
				t.Fatal(err)
			}
			turn, _ := s.Send(bg, core.User("pay bob"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			old := pendingCall(t, s, "c1")
			alice := thread.Approve("c1").Bind(old)
			alice.Who = "alice"
			if _, err := s.Decide(bg, alice); err != nil {
				t.Fatal(err)
			}
			edit := thread.Edit("c1", json.RawMessage(`{"to":"bob","amount":40}`)).Bind(old)
			edit.Who = "bob"
			if rt, err := s.Decide(bg, edit); err != nil || rt != nil {
				t.Fatalf("Decide(edit) = %v, %v; want recorded, nothing resumed", rt, err)
			}
			id := s.ID()
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, agent(wefttest.Say("done")), thread.Quorum(2))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			rev := pendingCall(t, s2, "c1")
			if rev.RevisionOf != old.ID || rev.ID == old.ID || rev.Quorum != 2 || string(rev.Args) != `{"amount":40,"to":"bob"}` {
				t.Fatalf("pending revision = %+v", rev)
			}
			again := thread.Approve("c1").Bind(rev)
			again.Who = "bob" // the editor again: one identity
			if rt, err := s2.Decide(bg, again); err != nil || rt != nil {
				t.Fatalf("the editor's second approval = %v, %v; want it counted once", rt, err)
			}
			carol := thread.Approve("c1").Bind(rev)
			carol.Who = "carol"
			rt, err := s2.Decide(bg, carol)
			if err != nil || rt == nil {
				t.Fatalf("the second approver = %v, %v; want the resume", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			if args, _ := pay.ran(); len(args) != 1 || args[0] != `{"to":"bob","amount":40}` {
				t.Fatalf("pay ran with %v", args)
			}
		})
	}
}

// A signed edit verifies, covers its edited arguments, survives a JSON
// round trip that escapes them, and is applied once. A recovery
// request takes no edit (rule D).
func TestSignedEditAndRecoveryRefusal(t *testing.T) {
	ring := matrixRing(t)
	pay := &payTool{}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":900}`}),
		wefttest.Say("done"),
	), pay.def()), thread.WithKeyring(ring), thread.AutoResume(false))
	if err != nil {
		t.Fatal(err)
	}
	turn, _ := s.Send(bg, core.User("pay"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	challenge, err := s.Request("c1")
	if err != nil {
		t.Fatal(err)
	}
	signed, err := ring.Sign(challenge, thread.Edit("c1", json.RawMessage(`{"to": "<bob>", "amount": 40}`)))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(signed) // escapes "<" in the raw arguments
	if err != nil {
		t.Fatal(err)
	}
	var back thread.SignedDecision
	if err := json.Unmarshal(wire, &back); err != nil {
		t.Fatal(err)
	}
	tampered := back
	tampered.EditedArgs = json.RawMessage(`{"to":"<bob>","amount":4000}`)
	if _, err := s.DecideSigned(bg, tampered); !errors.Is(err, thread.ErrBadSignature) {
		t.Fatalf("a tampered edit = %v, want ErrBadSignature", err)
	}
	if _, err := s.DecideSigned(bg, back); err != nil {
		t.Fatalf("the signed edit over JSON = %v", err)
	}
	if _, err := s.DecideSigned(bg, back); err != nil {
		t.Fatalf("the same signed edit again = %v, want it applied", err)
	}
	rt, err := s.Resume(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if args, _ := pay.ran(); len(args) != 1 || args[0] != `{"to":"<bob>","amount":40}` {
		t.Fatalf("pay ran with %v", args)
	}
}

// Rule D, and the edited key through a crash: a resume of an edited
// call that a crash cut off parks a recovery request on the edited
// arguments; an edit of it is refused; approving it retries the call
// as it ran — the edited arguments, the edited key. Both backends.
func TestEditedCallRecoveryKeepsItsKey(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			pay := &payTool{}
			agent := func(turns ...wefttest.Turn) *core.Agent { return core.New(wefttest.Script(turns...), pay.def()) }
			inner := b.open(t)
			cs := &crashStore{Storage: inner}
			s, err := thread.Create(bg, cs, agent(wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":900}`})),
				thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			turn, _ := s.Send(bg, core.User("pay"))
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Decide(bg, thread.Edit("c1", json.RawMessage(`{"to":"bob","amount":40}`)).Bind(pendingCall(t, s, "c1"))); err != nil {
				t.Fatal(err)
			}
			pay.crash = cs
			rt, err := s.Resume(bg)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = rt.Wait()
			_ = s.WaitIdle(bg)
			pay.mu.Lock()
			pay.crash = nil
			pay.mu.Unlock()
			id := s.ID()
			st := restart(t, b, inner, id)
			s2, err := thread.Open(bg, st, id, agent(wefttest.Say("done")), thread.AutoResume(false))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			if _, err := s2.RecoverTurn(bg); err != nil {
				t.Fatal(err)
			}
			r := pendingCall(t, s2, "c1")
			if !r.Recovery || string(r.Args) != `{"amount":40,"to":"bob"}` {
				t.Fatalf("recovery request = %+v, want the edited arguments", r)
			}
			if _, err := s2.Decide(bg, thread.Edit("c1", json.RawMessage(`{"to":"bob","amount":4}`)).Bind(r)); !errors.Is(err, thread.ErrInvalidDecision) {
				t.Fatalf("an edit of a recovery request = %v, want ErrInvalidDecision", err)
			}
			if _, err := s2.Decide(bg, thread.Approve("c1").Bind(r)); err != nil {
				t.Fatal(err)
			}
			rt, err = s2.Resume(bg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			args, keys := pay.ran()
			if len(args) != 2 || args[1] != `{"to":"bob","amount":40}` || keys[0] != keys[1] || r.IdempotencyKey != keys[0] {
				t.Fatalf("runs %v keys %v (request key %s): want the edited call retried under its key", args, keys, r.IdempotencyKey)
			}
		})
	}
}

// Cosigners (D2): a remote authority that counted its own quorum
// forwards one decision naming every decider. The MAC covers them, the
// decision entry records them, and weft never counts them toward its
// own Quorum.
func TestCosignersAreRecordedNotCounted(t *testing.T) {
	ring := matrixRing(t)
	pay := &payTool{}
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"to":"bob","amount":9}`}),
	), pay.def()), thread.WithKeyring(ring), thread.Quorum(2))
	if err != nil {
		t.Fatal(err)
	}
	turn, _ := s.Send(bg, core.User("pay"))
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	challenge, err := s.Request("c1")
	if err != nil {
		t.Fatal(err)
	}
	d := thread.Approve("c1")
	d.Cosigners = []string{"usr_alice", "usr_carol"}
	signed, err := ring.Sign(challenge, d)
	if err != nil {
		t.Fatal(err)
	}
	forged := signed
	forged.Cosigners = []string{"usr_alice", "usr_mallory"}
	if _, err := s.DecideSigned(bg, forged); !errors.Is(err, thread.ErrBadSignature) {
		t.Fatalf("changed cosigners = %v, want ErrBadSignature", err)
	}
	if rt, err := s.DecideSigned(bg, signed); err != nil || rt != nil {
		t.Fatalf("DecideSigned = %v, %v; want recorded and the quorum still open", rt, err)
	}
	if p := s.Pending(); len(p) != 1 {
		t.Fatalf("two cosigners counted toward the quorum: pending %+v", p)
	}
	var got []string
	for _, e := range s.Entries() {
		if de, ok := e.(thread.ApprovalDecisionEntry); ok && de.CallID == "c1" {
			got = de.Cosigners
		}
	}
	if strings.Join(got, ",") != "usr_alice,usr_carol" {
		t.Fatalf("recorded cosigners = %v", got)
	}
}
