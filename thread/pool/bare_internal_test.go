package pool

import (
	"context"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
)

// A panic out of the subagent's handler chain on the bare path (no
// session) gives the pool's slot back and the outer run's slot is taken
// again: on a pool of one, the next bare delegation runs.
func TestBarePanicReleasesTheSlot(t *testing.T) {
	ctx := context.Background()
	p := New(1)
	defer func() { _ = p.Close(ctx) }()

	// An outer bare run holding the one slot, as a nested call's caller.
	if err := p.sem.enqueue().wait(ctx); err != nil {
		t.Fatal(err)
	}
	outer := &slot{f: p.sem, ctx: ctx, held: true}
	octx := withRun(ctx, runInfo{depth: 1, slot: outer})

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the handler's panic did not propagate")
			}
		}()
		_, _ = p.bare(octx, core.ToolCallPart{ID: "c1", Name: "worker"},
			func(context.Context, core.ToolCallPart) (string, error) { panic("boom") })
	}()
	if !outer.holds() {
		t.Fatal("the outer run did not get its slot back after the inner panic")
	}
	outer.finish()

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res, err := p.bare(cctx, core.ToolCallPart{ID: "c2", Name: "worker"},
		func(context.Context, core.ToolCallPart) (string, error) { return "ok", nil })
	if err != nil || res != "ok" {
		t.Fatalf("second bare delegation = %q, %v; the slot leaked", res, err)
	}
}
