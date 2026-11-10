package thread_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// A storage write that hangs inside the dispatch gate — which holds the
// session's lock across the dispatch record's append — no longer makes
// Close ignore its context (review finding D2, R12): Close gives up
// when ctx ends, and the giving up (the turn felled, recorded) happens
// once the write returns. A later Close then finishes.
func TestCloseGivesUpWhileAStorageWriteHangs(t *testing.T) {
	hold := make(chan struct{})
	entered := make(chan struct{}, 1)
	st := &hangingDispatchStore{Storage: thread.Memory(), call: "w1", hold: hold, entered: entered}
	wire := core.Tool("wire", "Wire money.", func(context.Context, struct{}) (string, error) {
		return "wired", nil
	})
	s, err := thread.Create(bg, st, core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "wire", ID: "w1", Args: `{}`}),
		wefttest.Say("done"),
	), wire))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Send(bg, core.User("wire it")); err != nil {
		t.Fatal(err)
	}
	<-entered // the gate is inside the storage write, holding the session's lock
	ctx, cancel := context.WithCancel(bg)
	cancel()
	done := make(chan error, 1)
	go func() { done <- s.Close(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v, want the context's error", err)
		}
	case <-time.After(10 * time.Second): // a hang detector, not a pass condition
		close(hold)
		<-done
		t.Fatal("Close(ctx already ended) still blocked: the gate holds the session's lock across a hung write")
	}
	close(hold)
	if err := s.Close(bg); err != nil {
		t.Fatalf("the second Close = %v", err)
	}
}

type hangingDispatchStore struct {
	thread.Storage
	call    string
	hold    <-chan struct{}
	entered chan struct{}
}

func (s *hangingDispatchStore) Append(ctx context.Context, session string, entries ...thread.Entry) error {
	for _, e := range entries {
		if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepDispatch && a.CallID == s.call {
			select {
			case s.entered <- struct{}{}:
			default:
			}
			<-s.hold
		}
	}
	return s.Storage.Append(ctx, session, entries...)
}
