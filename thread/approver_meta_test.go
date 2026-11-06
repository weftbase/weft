package thread_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// The live Approver is handed the parking call's run metadata, as
// Pending and OnRequest are: before 0.24.1 its Request carried none,
// so an approver routing by tenant (or any run metadata) could not.
func TestApproverSeesTheRunMetadata(t *testing.T) {
	ctx := context.Background()
	refund := core.Tool("refund", "", func(context.Context, struct{}) (string, error) { return "ok", nil },
		core.RequireApproval())
	agt := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{}`}),
		wefttest.Say("done"),
	), refund)
	var mu sync.Mutex
	var seen map[string]string
	s, err := thread.Create(ctx, thread.Memory(), agt, thread.WithApprover(func(_ context.Context, r thread.Request) (thread.Decision, bool) {
		mu.Lock()
		seen = r.Metadata
		mu.Unlock()
		return thread.Decision{}, false
	}, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("go"), thread.RunOptions(core.Metadata(map[string]string{"tenant": "acme"})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["tenant"] != "acme" {
		t.Errorf("the Approver's Request.Metadata = %v, want the run's tenant", seen)
	}
	if p := s.Pending(); len(p) != 1 || p[0].Metadata["tenant"] != "acme" {
		t.Errorf("Pending = %+v, want the same metadata", p)
	}
}
