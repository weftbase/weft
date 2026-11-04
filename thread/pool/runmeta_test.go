package pool_test

import (
	"context"
	"maps"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// A pool child runs under its parent's run metadata, as a fork does:
// its header carries the parent's pairs, and its turns stamp them.
func TestChildInheritsRunMetadata(t *testing.T) {
	ctx := context.Background()
	st := thread.Memory()
	md := map[string]string{"weftbase.agent_version": "agv_7", "tenant": "acme"}
	parent, err := thread.Create(ctx, st, core.New(wefttest.Script()), thread.WithRunMetadata(md))
	if err != nil {
		t.Fatal(err)
	}
	var seen map[string]string
	child := core.New(wefttest.Script(wefttest.Say("ok")), core.OnRunEnd(func(ctx context.Context, _ *core.RunResult, _ error) {
		seen = core.MetadataFromContext(ctx)
	}))
	p := pool.New(1)
	defer func() { _ = p.Close(ctx) }()
	r, err := p.Submit(ctx, parent, child, "go")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Wait(ctx, parent, r.ID); err != nil {
		t.Fatal(err)
	}
	h, _, _, err := st.Load(ctx, r.Child)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(h.RunMeta, md) {
		t.Fatalf("child header run metadata = %v, want the parent's %v", h.RunMeta, md)
	}
	if seen["weftbase.agent_version"] != "agv_7" || seen["tenant"] != "acme" {
		t.Fatalf("child run metadata = %v, want the parent's pairs", seen)
	}
}
