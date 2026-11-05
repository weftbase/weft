package pool_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/thread/pool"
)

// acceptFirstRun runs one sync delegation whose child parks at an
// approval, in a jsonl store under dir, and returns the parent's id and
// the child's. The two images below cut it where a process can die
// around the child's creation: the acceptance receipt is written
// first, the child created after it.
func acceptFirstRun(t *testing.T, dir string) (top, child string, mkChild func() *core.Agent) {
	t.Helper()
	ctx := context.Background()
	st, err := jsonl.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	refund := core.Tool("refund", "", func(context.Context, struct{}) (string, error) { return "ok", nil }, core.RequireApproval())
	mkChild = func() *core.Agent {
		return core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "r1", Args: `{}`}),
			wefttest.Say("child done")), refund)
	}
	p := pool.New(2)
	s, err := thread.Create(ctx, st, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "w", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("parent done")), p.MustWrap("w", "w", mkChild())))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = turn.Wait()
	child = pool.Receipts(s)[0].Child
	top = s.ID()
	_ = p.Close(ctx)
	_ = s.Close(ctx)
	return top, child, mkChild
}

// lineageOrphans counts the sessions in dir whose lineage names top and
// that no receipt of top's names (Children misses them).
func lineageOrphans(t *testing.T, st thread.Storage, dir string, s *thread.Session) int {
	t.Helper()
	ctx := context.Background()
	files, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	n := 0
	for _, f := range files {
		id := strings.TrimSuffix(filepath.Base(f), ".jsonl")
		if id == s.ID() {
			continue
		}
		if h, _, _, err := st.Load(ctx, id); err == nil && h.Lineage != nil && h.Lineage.Session == s.ID() {
			n++
		}
	}
	kids, err := pool.Children(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	return n - len(kids)
}

// The image of a crash between the acceptance and the child's
// creation: a receipt naming a child that does not exist. Recover
// settles it failed saying nothing ran — never a guess — and no session
// carries the parent's lineage that the ledger does not name (WR-25).
func TestAcceptedChildNeverCreated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	top, child, mkChild := acceptFirstRun(t, dir)
	cutBefore(t, filepath.Join(dir, top+".jsonl"), after(kind("pool_receipt")))
	st, err := jsonl.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := thread.Delete(ctx, st, child); err != nil {
		t.Fatal(err)
	}
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	s, err := thread.Open(ctx, st, top, core.New(wefttest.Script(wefttest.Say("parent done")), p.MustWrap("w", "w", mkChild())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	if err := p.Recover(ctx, s); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	rs := pool.Receipts(s)
	want := fmt.Sprintf("child session %s does not exist and its run never started: nothing ran", child)
	if len(rs) != 1 || rs[0].State != pool.Failed || rs[0].Stop != want {
		t.Fatalf("receipts = %+v, want one failed with %q", rs, want)
	}
	if n := lineageOrphans(t, st, dir, s); n != 0 {
		t.Errorf("%d session(s) carry the parent's lineage and no receipt names them", n)
	}
}

// The image of a crash before the acceptance: nothing names a child and
// none exists. The delegating call goes to a person; approving its
// retry re-delegates from the same recorded call, so the child takes the
// same derived id (R3) — and no second, unnamed child appears.
func TestRecoveryRetryReachesTheSameChild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	top, child, mkChild := acceptFirstRun(t, dir)
	cutBefore(t, filepath.Join(dir, top+".jsonl"), kind("pool_receipt"))
	st, err := jsonl.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := thread.Delete(ctx, st, child); err != nil {
		t.Fatal(err)
	}
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	s, err := thread.Open(ctx, st, top, core.New(wefttest.Script(wefttest.Say("parent done")), p.MustWrap("w", "w", mkChild())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	if err := s.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Recover(ctx, s); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err := s.RecoverTurn(ctx); err != nil {
		t.Fatalf("RecoverTurn: %v", err)
	}
	pend := s.Pending()
	if len(pend) != 1 || pend[0].CallID != "d1" || !pend[0].Recovery {
		t.Fatalf("pending = %+v, want d1 as a recovery request", pend)
	}
	if err := p.Decide(ctx, s, bound(s, thread.Approve("d1"))); err != nil {
		t.Fatalf("approve d1: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(pool.Receipts(s)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the approved retry never delegated")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pool.Receipts(s)[0].Child; got != child {
		t.Errorf("the retry's child is %s, want the original call's derived id %s", got, child)
	}
	if n := lineageOrphans(t, st, dir, s); n != 0 {
		t.Errorf("%d session(s) carry the parent's lineage and no receipt names them", n)
	}
}

// noChildren refuses to create any session with a lineage: a child's
// creation that fails after its acceptance is written.
type noChildren struct{ thread.Storage }

func (s noChildren) Create(ctx context.Context, h thread.Header) error {
	if h.Lineage != nil {
		return fmt.Errorf("refused child %s", h.ID)
	}
	return s.Storage.Create(ctx, h)
}

// The acceptance comes before the child: a child whose creation fails
// leaves its receipt settled failed at once — nothing ran — and the
// model reads the creation's failure as the call's error.
func TestChildCreationFailureSettlesItsReceipt(t *testing.T) {
	ctx := context.Background()
	st := noChildren{thread.Memory()}
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	child := core.New(wefttest.Script(wefttest.Say("child done")))
	s, err := thread.Create(ctx, st, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "w", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("parent done")), p.MustWrap("w", "w", child)))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := turn.Wait()
	if err != nil {
		t.Fatal(err)
	}
	rs := pool.Receipts(s)
	if len(rs) != 1 || rs[0].State != pool.Failed || !strings.Contains(rs[0].Stop, "was not created") {
		t.Fatalf("receipts = %+v, want one failed: not created", rs)
	}
	found := false
	for _, m := range res.Messages {
		for _, part := range m.Content {
			if r, ok := part.(core.ToolResultPart); ok && r.CallID == "d1" && r.IsError && strings.Contains(r.Content, "refused child") {
				found = true
			}
		}
	}
	if !found {
		t.Error("the delegating call's result does not carry the creation failure")
	}
}
