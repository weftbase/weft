package thread_test

import (
	"context"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// DelegationLanded is thread/pool's wait for a delegating call's turn:
// no mirror is nothing to wait for; a mirror with no turn entry after
// it answers not landed and hands out a channel the session closes
// when its next turn entry is recorded — one channel for every waiter,
// closed once — after which the answer is landed.
func TestDelegationLandedNotifiesAtTheNextTurnEntry(t *testing.T) {
	ctx := context.Background()
	s, err := thread.Create(ctx, thread.Memory(), core.New(wefttest.Script(wefttest.Say("done"))))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	if landed, next := s.DelegationLanded("s_child", "w1"); !landed || next != nil {
		t.Fatalf("with no mirror: landed=%v, next=%v; want landed and nothing to wait on", landed, next)
	}
	if _, err := s.AppendApprovalRequests(ctx, thread.ApprovalRequestEntry{
		CallID: "s_child/c1", Tool: "refund", Child: "s_child", Wrapper: "w1", RunID: "s_child-t1",
	}); err != nil {
		t.Fatal(err)
	}
	landed, next := s.DelegationLanded("s_child", "w1")
	if landed || next == nil {
		t.Fatalf("mirrored, no turn entry: landed=%v, next=%v; want not landed and a channel", landed, next)
	}
	if _, again := s.DelegationLanded("s_child", "w1"); again != next {
		t.Error("two waiters on one turn were handed two channels")
	}
	select {
	case <-next:
		t.Fatal("the notification fired with no turn entry recorded")
	default:
	}
	if err := s.DenyMirrored(ctx, "s_child", "test: clear the boundary"); err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(ctx, core.User("go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-next:
	case <-time.After(5 * time.Second):
		t.Fatal("the turn entry landed and the notification never fired")
	}
	if landed, _ := s.DelegationLanded("s_child", "w1"); !landed {
		t.Error("after the turn entry: not landed")
	}
}
