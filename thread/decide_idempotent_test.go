package thread_test

import (
	"errors"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// Deciding is idempotent and bound (G3, G4, WR-12). Decide refuses a
// decision that names no request (ErrInvalidDecision). The same bound
// decision again — before and after a restart, unsigned or signed with
// the same bytes — is applied: nil, nothing recorded, nothing audited.
// A different decision on the decided request is ErrNotPending, a
// different one under a spent nonce ErrReplay. Both backends.
func TestDecidingTwiceIsANoOp(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			ring := matrixRing(t)
			agent := func(turns ...wefttest.Turn) *core.Agent {
				a, _ := refundAgent(turns...)
				return a
			}
			st := b.open(t)
			s, err := thread.Create(bg, st, agent(wefttest.ToolCalls(
				wefttest.Call{Name: "refund", ID: "c1", Args: `{"order_id":"1"}`},
				wefttest.Call{Name: "refund", ID: "c2", Args: `{"order_id":"2"}`},
			)), thread.AutoResume(false), thread.WithKeyring(ring))
			if err != nil {
				t.Fatal(err)
			}
			turn, err := s.Send(bg, core.User("refund both"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.Wait(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Decide(bg, thread.Approve("c1")); !errors.Is(err, thread.ErrInvalidDecision) {
				t.Fatalf("an unbound Decide = %v, want ErrInvalidDecision", err)
			}
			var r1 thread.Request
			for _, r := range s.Pending() {
				if r.CallID == "c1" {
					r1 = r
				}
			}
			d := thread.Approve("c1").Bind(r1)
			if _, err := s.Decide(bg, d); err != nil {
				t.Fatal(err)
			}
			challenge, err := s.Request("c2")
			if err != nil {
				t.Fatal(err)
			}
			signed, err := ring.Sign(challenge, thread.Deny("c2", "no"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.DecideSigned(bg, signed); err != nil {
				t.Fatal(err)
			}
			id := s.ID()
			count := func(s *thread.Session) int {
				n := 0
				for _, e := range s.Entries() {
					switch e.(type) {
					case thread.ApprovalDecisionEntry, thread.ApprovalAuditEntry:
						n++
					}
				}
				return n
			}
			before := count(s)
			if _, err := s.Decide(bg, d); err != nil {
				t.Fatalf("the same decision again = %v, want nil", err)
			}
			st = restart(t, b, st, id)
			s2, err := thread.Open(bg, st, id, agent(wefttest.Say("done")), thread.AutoResume(false), thread.WithKeyring(ring))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s2.Close(bg) }()
			if _, err := s2.Decide(bg, d); err != nil {
				t.Fatalf("the same decision after a restart = %v, want nil", err)
			}
			if _, err := s2.DecideSigned(bg, signed); err != nil {
				t.Fatalf("the same signed decision after a restart = %v, want nil", err)
			}
			if got := count(s2); got != before {
				t.Fatalf("redeliveries wrote %d entries", got-before)
			}
			other := d
			other.Reason, other.Kind = "changed my mind", thread.OutcomeDeny
			if _, err := s2.Decide(bg, other); !errors.Is(err, thread.ErrNotPending) {
				t.Fatalf("another decision on the decided request = %v, want ErrNotPending", err)
			}
			if _, err := s2.DecideSigned(bg, thread.SignDecision([]byte("matrix secret, thirty-two bytes!"), challenge, thread.Approve("c2"))); !errors.Is(err, thread.ErrReplay) {
				t.Fatalf("another decision under the spent nonce = %v, want ErrReplay", err)
			}
		})
	}
}
