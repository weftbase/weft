package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/pool"
)

// slowChildren blocks the creation of any session with a lineage until
// release is closed.
type slowChildren struct {
	thread.Storage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newSlowChildren() *slowChildren {
	return &slowChildren{Storage: thread.Memory(), entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *slowChildren) Create(ctx context.Context, h thread.Header) error {
	if h.Lineage != nil {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.Storage.Create(ctx, h)
}

// Cancel of an accepted delegation whose child this pool is still
// creating (the acceptance comes first) waits for the registration and
// cancels it — it used to answer an Orphan StateError telling the
// caller to Recover, which skips a live creation, and the child then
// ran to done. Recover beside the creation leaves it alone.
func TestCancelWhileTheChildIsCreated(t *testing.T) {
	ctx := context.Background()
	st := newSlowChildren()
	p := pool.New(2)
	defer func() { _ = p.Close(ctx) }()
	// The child's run holds until it is canceled: only a Cancel ends it.
	hold := core.Tool("hold", "Hold.", func(ctx context.Context, _ struct{}) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	child := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "hold", ID: "h1", Args: `{}`}),
		wefttest.Say("child done")), hold)
	s, err := thread.Create(ctx, st, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "w", ID: "d1", Args: wefttest.Args(struct{ Prompt string }{"go"})}),
		wefttest.Say("parent done")), p.MustWrap("w", "w", child)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	<-st.entered
	rs := pool.Receipts(s)
	if len(rs) != 1 || rs[0].State != pool.Accepted {
		t.Fatalf("receipts while the child is created = %+v", rs)
	}
	if err := p.Recover(ctx, s); err != nil {
		t.Fatalf("Recover beside the creation: %v", err)
	}
	if rs := pool.Receipts(s); rs[0].Settled() {
		t.Fatalf("Recover settled a delegation whose child is being created: %+v", rs[0])
	}
	cerr := make(chan error, 1)
	go func() { cerr <- p.Cancel(ctx, s, rs[0].ID) }()
	select {
	case err := <-cerr:
		t.Fatalf("Cancel returned %v before the child was created; want it to wait", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(st.release)
	if err := <-cerr; err != nil {
		t.Fatalf("Cancel of a delegation whose child was being created = %v", err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	got, err := p.Wait(ctx, s, rs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != pool.Canceled {
		t.Fatalf("receipt after the Cancel = %+v, want canceled", got)
	}
}
