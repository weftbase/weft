package pool_test

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// blockModel blocks its call until its context ends.
type blockModel struct {
	core.Model
	in chan struct{}
}

func (m blockModel) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	return func(yield func(core.ModelEvent, error) bool) {
		close(m.in)
		<-ctx.Done()
		yield(nil, ctx.Err())
	}
}

// pool.Close under a context canceled with thread.ErrShutdown cuts a
// running child off instead of canceling it: the child's turn is
// recorded cut off and its receipt is left unsettled, for Recover
// after the restart. A plain Close still settles it canceled.
func TestPoolCloseUnderShutdownCutsChildrenOff(t *testing.T) {
	for _, shutdown := range []bool{true, false} {
		ctx := context.Background()
		st := thread.Memory()
		parent, err := thread.Create(ctx, st, core.New(wefttest.Script()))
		if err != nil {
			t.Fatal(err)
		}
		in := make(chan struct{})
		p := pool.New(1)
		r, err := p.Submit(ctx, parent, core.New(blockModel{Model: wefttest.Script(), in: in}), "go")
		if err != nil {
			t.Fatal(err)
		}
		<-in
		cctx, cancel := context.WithCancelCause(ctx)
		if shutdown {
			cancel(thread.ErrShutdown)
		} else {
			cancel(nil)
		}
		_ = p.Close(cctx)
		wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
		_ = p.Close(wctx)
		wcancel()
		time.Sleep(50 * time.Millisecond)
		child, err := thread.Open(ctx, st, r.Child, core.New(wefttest.Script()))
		if err != nil {
			t.Fatal(err)
		}
		var te *thread.TurnEntry
		for _, e := range child.Entries() {
			if x, ok := e.(thread.TurnEntry); ok {
				te = &x
			}
		}
		rcs := pool.Receipts(parent)
		if shutdown {
			if te == nil || !te.CutOff || te.Canceled {
				t.Fatalf("shutdown: child turn = %+v, want cut off", te)
			}
			if len(rcs) != 1 || rcs[0].State == pool.Canceled {
				t.Fatalf("shutdown: receipts = %+v, want unsettled", rcs)
			}
		} else if len(rcs) != 1 || rcs[0].State != pool.Canceled {
			t.Fatalf("plain close: receipts = %+v, want canceled", rcs)
		}
	}
}
