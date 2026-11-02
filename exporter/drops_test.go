package exporter

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/otel"
	"github.com/weftgo/weft/wefttest"
)

// stormAgent is a run of steps steps, each streaming deltas text deltas
// and calls tool calls.
func stormAgent(steps, deltas, calls int, opts ...core.Option) *core.Agent {
	echo := core.Tool("echo", "Echoes.", func(context.Context, struct{}) (string, error) { return "ok", nil })
	turns := make([]wefttest.Turn, 0, steps+1)
	for s := range steps {
		evs := make([]core.ModelEvent, 0, deltas+calls+1)
		for range deltas {
			evs = append(evs, core.ModelTextDelta{Text: "x"})
		}
		for c := range calls {
			evs = append(evs, core.ModelToolCall{ID: fmt.Sprintf("c%d_%d", s, c), Name: "echo", Args: []byte(`{}`)})
		}
		evs = append(evs, core.ModelFinish{Reason: core.StopToolCalls, Usage: core.Usage{InputTokens: 1, OutputTokens: 1}})
		turns = append(turns, wefttest.Raw(evs...))
	}
	turns = append(turns, wefttest.Say("done"))
	return core.New(wefttest.Script(turns...), append([]core.Option{core.Name("storm"), echo, core.MaxSteps(steps + 1)}, opts...)...)
}

// TestLocalFloodLosesNothing (moved from weft/otel): the Local
// destination writes each record
// synchronously on the emitting goroutine — no queue — so a storm costs
// it time, never records: every event the root reports emitted is
// stored, and nothing is counted lost.
func TestLocalFloodLosesNothing(t *testing.T) {
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(), otel.Heartbeat(0),
		Local(filepath.Join(t.TempDir(), "weft.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(testCtx(t)) }()
	res, err := stormAgent(10, 200, 3, core.TracerProvider(p.TracerProvider()),
		core.LoggerProvider(p.LoggerProvider())).Generate(context.Background(), core.Prompt("storm"))
	if err != nil {
		t.Fatal(err)
	}
	det, err := LocalDBOf(p).Run(context.Background(), res.ID)
	if err != nil {
		t.Fatal(err)
	}
	if det.EventsEmitted == 0 || det.EventCount != det.EventsEmitted {
		t.Errorf("stored %d of %d emitted events: Local lost records", det.EventCount, det.EventsEmitted)
	}
}
