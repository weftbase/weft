package thread_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/wefttest"
)

// tornTurnEnd writes only the first keep complete lines of the batch
// that carries the turn entry of run — what a SIGKILL inside a
// multi-page write(2), or a power cut, leaves under jsonl: whole prefix
// lines, the rest gone — and the process is gone from then on. size
// reports the batch's length.
type tornTurnEnd struct {
	thread.Storage
	run  string
	keep int
	size atomic.Int32
	down atomic.Bool
}

func (c *tornTurnEnd) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	if c.down.Load() {
		return errors.New("the process is gone")
	}
	for _, e := range entries {
		if te, ok := e.(thread.TurnEntry); ok && te.RunID == c.run {
			c.down.Store(true)
			c.size.Store(int32(len(entries)))
			var raw []byte
			for _, w := range entries[:min(c.keep, len(entries))] {
				line, err := json.Marshal(w)
				if err != nil {
					return err
				}
				raw = append(append(raw, line...), '\n')
			}
			if len(raw) > 0 {
				inj := c.Storage.(interface {
					Inject(context.Context, string, []byte) error
				})
				if err := inj.Inject(ctx, session, raw); err != nil {
					return err
				}
			}
			return errors.New("the process is gone")
		}
	}
	return c.Storage.Append(ctx, session, entries...)
}

// A turn's end batch torn after any prefix of complete lines (review
// R4): jsonl keeps the prefix, so the turn entry goes last in the batch
// — a prefix never holds the turn's end without the recovery request
// the in-process ladder wrote for its call of unknown outcome. Every
// prefix reads as the turn cut off: Send is refused, RecoverTurn parks
// the call as a recovery request, and a skip reads the OUTCOME_UNKNOWN
// text, never "DENIED".
func TestTornTurnEndBatchReadsCutOff(t *testing.T) {
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, in orderArg) (string, error) {
		time.Sleep(150 * time.Millisecond) // lands after its Timeout
		return "paid", nil
	}, core.Timeout(30*time.Millisecond))
	size := 2 // learned from the first case
	for keep := 0; keep < size; keep++ {
		t.Run(fmt.Sprintf("keep_%d", keep), func(t *testing.T) {
			dir := t.TempDir()
			inner, err := jsonl.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			st := &tornTurnEnd{Storage: inner, keep: keep}
			s, err := thread.Create(bg, st, core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_1", Args: `{"order_id":"A"}`}),
			), pay))
			if err != nil {
				t.Fatal(err)
			}
			id := s.ID()
			st.run = id + "-t1"
			turn, err := s.Send(bg, core.User("pay"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := turn.WaitContext(deadline(t, 5*time.Second)); !errors.Is(err, thread.ErrNotPersisted) {
				t.Fatalf("turn: %v, want ErrNotPersisted", err)
			}
			if n := int(st.size.Load()); n > size {
				size = n
			}
			if size < 2 {
				t.Fatalf("the turn's end batch holds %d entries; the test needs the ladder's request in it", size)
			}
			abandon(t, inner, id)
			closeStore(inner)

			st2, err := jsonl.Open(dir) // the next process
			if err != nil {
				t.Fatal(err)
			}
			defer closeStore(st2)
			model := wefttest.Script(wefttest.Say("ok"))
			s2, err := thread.Open(bg, st2, id, core.New(model, pay))
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close(bg)
			if _, err := s2.Send(bg, core.User("did it pay?")); !errors.Is(err, thread.ErrRecoveryRequired) {
				t.Fatalf("Send = %v, want ErrRecoveryRequired", err)
			}
			if _, err := s2.RecoverTurn(bg); err != nil {
				t.Fatalf("RecoverTurn: %v", err)
			}
			p := s2.Pending()
			if len(p) != 1 || !p[0].Recovery {
				t.Fatalf("Pending = %+v, want call_1 as one recovery request", p)
			}
			rt, err := s2.Decide(bg, bound(s2, thread.Deny("call_1", "no")))
			if err != nil || rt == nil {
				t.Fatalf("Decide(deny) = %v, %v", rt, err)
			}
			if _, err := rt.WaitContext(deadline(t, 5*time.Second)); err != nil {
				t.Fatal(err)
			}
			got, _ := toolResultIn(model.Requests()[0], "call_1")
			if !strings.HasPrefix(got, core.CodeOutcomeUnknown+": ") {
				t.Errorf("the model reads call_1 as %q, want the OUTCOME_UNKNOWN skip", got)
			}
		})
	}
}
