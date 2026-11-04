package thread_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/sqlite"
	"github.com/weftbase/weft/wefttest"
)

// WR-4: a writer that lost its lock starts no side effect, even for a
// call whose step it wrote while it still held the lock. A step holds
// a ReplaySafe call that waits and a risky call queued behind it
// (Sequential); while the first waits, the writer's lock is lost —
// released (jsonl, sqlite) or broken by an operator (sqlite
// BreakLock) — and another worker takes the session over. The risky
// call is then refused by the gate's self-fence: its handler never
// runs and the model reads ErrNotDispatched. Both backends.
func TestLostLeaseStartsNoSideEffect(t *testing.T) {
	type loss struct {
		name string
		lose func(t *testing.T, st thread.Storage, id string)
	}
	losses := []loss{{"released", func(t *testing.T, st thread.Storage, id string) { abandon(t, st, id) }}}
	for _, b := range durableBackends(t) {
		ls := losses
		if b.name == "sqlite" {
			ls = append(ls, loss{"broken", func(t *testing.T, st thread.Storage, id string) {
				if err := sqlite.BreakLock(bg, st, id); err != nil {
					t.Fatal(err)
				}
			}})
		}
		for _, l := range ls {
			t.Run(b.name+"/"+l.name, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				wait := core.Tool("wait", "Wait.", func(ctx context.Context, _ struct{}) (string, error) {
					close(entered)
					<-release
					return "waited", nil
				}, core.Replay(core.ReplaySafe))
				charge := &sideEffect{}
				stA := b.open(t)
				a, err := thread.Create(bg, stA, core.New(wefttest.Script(
					wefttest.ToolCalls(
						wefttest.Call{Name: "wait", ID: "w", Args: `{}`},
						wefttest.Call{Name: "charge", ID: "c", Args: `{"order_id":"1"}`},
					),
					wefttest.Say("A done"),
				), wait, charge.def(core.Sequential())))
				if err != nil {
					t.Fatal(err)
				}
				turn, err := a.Send(bg, core.User("go"))
				if err != nil {
					t.Fatal(err)
				}
				<-entered
				l.lose(t, stA, a.ID())
				stB := b.open(t)
				bs, err := thread.Open(bg, stB, a.ID(), core.New(wefttest.Script()))
				if err != nil {
					t.Fatal(err)
				}
				if err := bs.Claim(bg); err != nil {
					t.Fatalf("B's Claim = %v", err)
				}
				close(release)
				res, _ := turn.Wait()
				if n := len(charge.ran()); n != 0 {
					t.Fatalf("the fenced-off writer ran the charge %d times", n)
				}
				if res != nil {
					if r := toolResult(res, "c"); !strings.Contains(r, core.ErrNotDispatched.Error()) {
						t.Fatalf("the model saw %q, want ErrNotDispatched", r)
					}
				}
				_ = bs.Close(bg)
			})
		}
	}
}

var _ = errors.Is
