package thread_test

import (
	"context"
	"maps"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// A parked request records the run metadata its run ran under — the
// session's (WithRunMetadata) under the Send's own — so a process that
// reopens the session under other run metadata (another deployment,
// another version label) still reads what the parked run was started
// with, as Request.Metadata; and the resume runs under it, the parked
// pairs winning. Both file backends, a fresh store for the reopen.
func TestParkedRequestRecordsRunMetadata(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) { parkedRequestRecordsRunMetadata(t, b) })
	}
}

func parkedRequestRecordsRunMetadata(t *testing.T, b durableBackend) {
	var seen map[string]string
	shipAgent := func(turns ...wefttest.Turn) *core.Agent {
		ship := core.Tool("ship", "Ship an order.",
			func(ctx context.Context, in refundInput) (string, error) {
				seen = core.MetadataFromContext(ctx)
				return "shipped " + in.OrderID, nil
			},
			core.RequireApproval())
		return core.New(wefttest.Script(turns...), core.Name("meta"), ship)
	}
	st := b.open(t)
	s, err := thread.Create(bg, st, shipAgent(wefttest.ToolCalls(wefttest.Call{Name: "ship", ID: "call_1", Args: `{"order_id":"1"}`})),
		thread.WithRunMetadata(map[string]string{"weftbase.agent_version": "agv_1", "tenant": "acme", "region": "eu"}))
	if err != nil {
		t.Fatal(err)
	}
	turn, err := s.Send(bg, core.User("ship it"), thread.RunOptions(core.Metadata(map[string]string{"tenant": "acme-2"})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"weftbase.agent_version": "agv_1", "tenant": "acme-2", "region": "eu"}
	if p := s.Pending(); len(p) != 1 || !maps.Equal(p[0].Metadata, want) {
		t.Fatalf("pending = %+v, want metadata %v", p, want)
	}
	id := s.ID()

	st = restart(t, b, st, id)
	s2, err := thread.Open(bg, st, id, shipAgent(wefttest.Say("done")),
		thread.WithRunMetadata(map[string]string{"weftbase.agent_version": "agv_2"}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close(bg) }()
	p := s2.Pending()
	if len(p) != 1 || !maps.Equal(p[0].Metadata, want) {
		t.Fatalf("pending after reopen = %+v, want metadata %v", p, want)
	}
	p[0].Metadata["tenant"] = "mutated"
	if again := s2.Pending(); again[0].Metadata["tenant"] != "acme-2" {
		t.Fatal("Request.Metadata is not a copy")
	}
	rt, err := s2.Decide(bg, bound(s2, thread.Approve("call_1")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Wait(); err != nil {
		t.Fatal(err)
	}
	if seen["weftbase.agent_version"] != "agv_1" || seen["tenant"] != "acme-2" || seen["region"] != "eu" {
		t.Fatalf("resumed call ran under %v, want the parked run's metadata", seen)
	}
}

// A send queued behind a running turn records the session's run
// metadata on its receipt beside its own, so the receipt restored by a
// reopen runs under the labels it was accepted with.
func TestQueuedReceiptRecordsRunMetadata(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := core.Tool("slow", "Slow.", func(ctx context.Context, _ struct{}) (string, error) {
		started <- struct{}{}
		<-release
		return "ok", nil
	}, core.Replay(core.ReplaySafe))
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "slow", ID: "c1", Args: `{}`}),
		wefttest.Say("one"),
		wefttest.Say("two"),
	), slow)
	s, err := thread.Create(bg, thread.Memory(), agent,
		thread.WithRunMetadata(map[string]string{"tenant": "acme", "region": "eu"}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	first, err := s.Send(bg, core.User("one"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	second, err := s.Send(bg, core.User("two"), thread.RunOptions(core.Metadata(map[string]string{"region": "us"})))
	if err != nil {
		t.Fatal(err)
	}
	var rc *thread.ReceiptEntry
	for _, e := range s.Entries() {
		if r, ok := e.(thread.ReceiptEntry); ok && r.Status == thread.ReceiptAccepted {
			rc = &r
		}
	}
	close(release)
	if _, err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	if rc == nil || rc.Run == nil {
		t.Fatalf("no accepted receipt with run settings: %+v", rc)
	}
	if want := map[string]string{"tenant": "acme", "region": "us"}; !maps.Equal(rc.Run.Metadata, want) {
		t.Fatalf("receipt metadata = %v, want %v", rc.Run.Metadata, want)
	}
}
