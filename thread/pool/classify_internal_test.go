package pool

import (
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
)

// A settlement Recover reads from a child's file bills the child's
// subtree as a live settlement does: its turns, its summaries, and its
// own settled delegations — each counted once (the first settlement
// of a receipt; progress entries carry none).
func TestClassifyBillsTheSubtree(t *testing.T) {
	u := func(n int64) core.Usage { return core.Usage{InputTokens: n} }
	entries := []thread.Entry{
		thread.PoolReceiptEntry{ID: "r1", Status: thread.PoolAccepted, Child: "g"},
		thread.PoolReceiptEntry{Receipt: "r1", Status: thread.PoolRunning, Child: "g"},
		thread.PoolReceiptEntry{Receipt: "r1", Status: thread.PoolDone, Child: "g", Usage: u(7)},
		thread.CompactionEntry{SummarizerUsage: u(2)},
		thread.TurnEntry{RunID: "run1", Usage: u(10)},
	}
	if _, _, got, _ := classify(entries); got != u(19) {
		t.Errorf("classify usage = %+v, want %+v", got, u(19))
	}
}
