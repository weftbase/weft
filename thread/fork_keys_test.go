package thread_test

import (
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// A Fork of a session a crash cut off carries the cut-off call's key
// (ADR 0032, 2026-11-10): the fork's RecoverTurn retries a ReplayKeyed
// call under the key it ran under in the origin — a receiver that
// deduplicates applies it once — not under a key derived from the
// fork's own session id (review finding: the effect ran twice, no
// person deciding).
func TestForkRecoverTurnRetriesUnderTheOriginsKey(t *testing.T) {
	for _, b := range durableBackends(t) {
		t.Run(b.name, func(t *testing.T) {
			charge := &sideEffect{}
			id, st := crashTurn(t, b, func(cs *crashStore) *core.Agent {
				charge.crash = cs
				return core.New(wefttest.Script(charged("charge")), charge.def(core.Replay(core.ReplayKeyed)))
			}, nil)
			charge.crash = nil
			st = restart(t, b, st, id)
			s, err := thread.Open(bg, st, id, core.New(wefttest.Script(wefttest.Say("done")), charge.def(core.Replay(core.ReplayKeyed))))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close(bg) }()
			f, err := s.Fork(bg, s.Leaf())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close(bg) }()
			rt, err := f.RecoverTurn(bg)
			if err != nil || rt == nil {
				t.Fatalf("fork RecoverTurn = %v, %v", rt, err)
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatal(err)
			}
			keys := charge.ran()
			if len(keys) != 2 || keys[0] != keys[1] {
				t.Fatalf("keys %v: the fork's keyed retry must run under the origin's key", keys)
			}
		})
	}
}
