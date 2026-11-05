package thread

import (
	"context"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// R13: the runner's per-item cut-off check reads the whole session
// only when something since the last read could have cut a run off. A
// run of queued turns that each end with their turn entry keeps the
// answer (cutOffClear, no run open), so the check costs nothing per
// turn; a navigation (Branch) clears it and the next check reads the
// file again — and finds the same answer the full read gives.
func TestCutOffCheckIsNotRereadPerTurn(t *testing.T) {
	ctx := context.Background()
	s, err := Create(ctx, Memory(), core.New(wefttest.Script(
		wefttest.Say("one"), wefttest.Say("two"), wefttest.Say("three"), wefttest.Say("four"),
	)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	var turns []*Turn
	for _, p := range []string{"a", "b", "c"} {
		turn, err := s.Send(ctx, core.User(p))
		if err != nil {
			t.Fatal(err)
		}
		turns = append(turns, turn)
	}
	for _, turn := range turns {
		if _, err := turn.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	clear, open := s.cutOffClear, len(s.runsOpen)
	_, full, _ := s.readCutOffLocked()
	s.mu.Unlock()
	if !clear || open != 0 {
		t.Fatalf("after three ended turns: cutOffClear=%v, %d runs open; want the answer kept", clear, open)
	}
	if full {
		t.Fatal("the full read finds a cut-off run the gate does not")
	}
	es := s.Entries()
	if err := s.Branch(ctx, idOf(es[0])); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	clear = s.cutOffClear
	s.mu.Unlock()
	if clear {
		t.Fatal("a Branch kept the cut-off answer: the next check would not read the file")
	}
	turn, err := s.Send(ctx, core.User("d"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turn.Wait(); err != nil {
		t.Fatal(err)
	}
}

// A run started and not ended by a turn entry keeps the check reading
// the file: the gate never answers "none" for an open run.
func TestCutOffGateKeepsAnOpenRunOpen(t *testing.T) {
	s := &Session{cutOffClear: true, leaf: "e1"}
	s.noteCutOffLocked(MessageEntry{ID: "e2", ParentID: "e1", RunID: "r1"})
	s.leaf = "e2"
	if len(s.runsOpen) != 1 {
		t.Fatalf("runsOpen = %v after a run's message", s.runsOpen)
	}
	s.noteCutOffLocked(TurnEntry{ID: "e3", ParentID: "e2", RunID: "r1", ReRun: "r2"})
	s.leaf = "e3"
	if !s.runsOpen["r2"] || s.runsOpen["r1"] {
		t.Fatalf("runsOpen = %v after an overflow's turn entry; want its re-run open", s.runsOpen)
	}
	s.noteCutOffLocked(TurnEntry{ID: "e4", ParentID: "e3", RunID: "r2", CutOff: true})
	if s.cutOffClear {
		t.Fatal("a CutOff turn entry kept the answer")
	}
}
