package thread_test

import (
	"errors"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// The arming matrix (G5): a resume run cut off while its approved
// ReplayNever call ran, then every path that can start a run over the
// session after the restart. Each one refuses with ErrRecoveryRequired
// and the call's handler is not run a second time; RecoverTurn then
// parks it as a recovery request, and skipping it lets the boundary
// resume — still with the handler run once. The pool's replay path
// (thread/pool: TestNestedRestartMidApprovedCallParksForRecovery) and
// the resume runner's own check (every armed resume passes it before
// its run starts) close the set. Both file backends.
func TestEveryArmingPathRefusesAnUnknownOutcome(t *testing.T) {
	paths := []struct {
		name string
		arm  func(t *testing.T, s *thread.Session) error
	}{
		{"resume", func(t *testing.T, s *thread.Session) error { _, err := s.Resume(bg); return err }},
		{"decide", func(t *testing.T, s *thread.Session) error {
			_, err := s.Decide(bg, bound(s, thread.Deny("c2", "no")))
			return err
		}},
		{"decide_signed", func(t *testing.T, s *thread.Session) error {
			r, err := s.Request("c2")
			if err != nil {
				t.Fatal(err)
			}
			sd, err := matrixRing(t).Sign(r, thread.Deny("c2", "no"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.DecideSigned(bg, sd)
			return err
		}},
		{"send", func(t *testing.T, s *thread.Session) error { _, err := s.Send(bg, core.User("and?")); return err }},
		{"continue", func(t *testing.T, s *thread.Session) error { _, err := s.Continue(bg); return err }},
	}
	for _, b := range durableBackends(t) {
		for _, path := range paths {
			t.Run(b.name+"/"+path.name, func(t *testing.T) {
				charge := &sideEffect{}
				wire := &sideEffect{name: "wire"}
				tools := func() []core.Option {
					return []core.Option{charge.def(core.RequireApproval()), wire.def(core.RequireApproval())}
				}
				inner := b.open(t)
				cs := &crashStore{Storage: inner}
				s, err := thread.Create(bg, cs, core.New(wefttest.Script(wefttest.ToolCalls(
					wefttest.Call{Name: "charge", ID: "c1", Args: `{"order_id":"1"}`},
					wefttest.Call{Name: "wire", ID: "c2", Args: `{"order_id":"2"}`},
				)), tools()...), thread.AutoResume(false), thread.WithKeyring(matrixRing(t)))
				if err != nil {
					t.Fatal(err)
				}
				turn, err := s.Send(bg, core.User("pay"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := turn.Wait(); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Decide(bg, bound(s, thread.Approve("c1"))); err != nil {
					t.Fatal(err)
				}
				charge.crash = cs
				rt, err := s.Resume(bg) // c1 runs, the process dies inside it
				if err != nil {
					t.Fatal(err)
				}
				_, _ = rt.Wait()
				_ = s.WaitIdle(bg)
				charge.crash = nil
				id := s.ID()
				st := restart(t, b, inner, id)

				s2, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("settled")), tools()...),
					thread.WithKeyring(matrixRing(t)))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s2.Close(bg) }()
				if err := path.arm(t, s2); !errors.Is(err, thread.ErrRecoveryRequired) {
					t.Fatalf("%s after the cut-off resume = %v, want ErrRecoveryRequired", path.name, err)
				}
				_ = s2.WaitIdle(bg)
				if n := len(charge.ran()); n != 1 {
					t.Fatalf("%s: the approved call ran %d times, want once", path.name, n)
				}
				if rt, err := s2.RecoverTurn(bg); err != nil || rt != nil {
					t.Fatalf("RecoverTurn = %v, %v; want the call parked", rt, err)
				}
				var ds []thread.Decision
				for _, r := range s2.Pending() {
					if r.CallID == "c1" && !r.Recovery {
						t.Fatalf("c1 parked as %+v, want a recovery request", r)
					}
					ds = append(ds, thread.Deny(r.CallID, "skip"))
				}
				if len(ds) == 0 {
					t.Fatal("nothing pending after RecoverTurn")
				}
				rt, err = s2.Decide(bg, boundAll(s2, ds...)...)
				if err != nil {
					t.Fatal(err)
				}
				if rt == nil {
					if rt, err = s2.Resume(bg); err != nil {
						t.Fatal(err)
					}
				}
				if res, err := rt.Wait(); err != nil || res.Text() != "settled" {
					t.Fatalf("the resume after recovery = %v, %v", res, err)
				}
				if n := len(charge.ran()); n != 1 || len(wire.ran()) != 0 {
					t.Fatalf("charge ran %d times, wire %d; want 1 and 0", n, len(wire.ran()))
				}
			})
		}
	}
}

func matrixRing(t *testing.T) *thread.Keyring {
	t.Helper()
	ring, err := thread.NewKeyring(thread.Key{ID: "k1", Secret: []byte("matrix secret, thirty-two bytes!"), Active: true})
	if err != nil {
		t.Fatal(err)
	}
	return ring
}
