package core_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// S2: ServeCall honours the agent's ParkSource (ADR 0030, amendment
// 2026-11-10). A call a source names — or any call, when a source
// panicked (its set is unknown: fail closed, as in a run) — returns the
// same ErrApprovalRequired refusal a RequireApproval tool gets there,
// and the handler never runs. A source naming another tool, or an
// empty one, leaves the call served.
func TestServeCallHonoursParkSource(t *testing.T) {
	cases := []struct {
		name   string
		src    func(context.Context) []string
		parked bool
	}{
		{"names_the_tool", func(context.Context) []string { return []string{"refund"} }, true},
		{"panics", func(context.Context) []string { panic("breakpoint store down") }, true},
		{"names_another", func(context.Context) []string { return []string{"lookup"} }, false},
		{"empty", func(context.Context) []string { return nil }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ran atomic.Int32
			refund := core.Tool("refund", "r", func(context.Context, struct{}) (string, error) {
				ran.Add(1)
				return "refunded", nil
			})
			agt := core.New(wefttest.Script(wefttest.Say("unused")), refund, core.ParkSource(c.src))
			out, err := agt.ServeCall(context.Background(), core.ToolCallPart{ID: "c1", Name: "refund", Args: []byte(`{}`)}, "")
			if c.parked {
				if !errors.Is(err, core.ErrApprovalRequired) || out != "" {
					t.Errorf("ServeCall = %q, %v; want the ErrApprovalRequired refusal", out, err)
				}
				if n := ran.Load(); n != 0 {
					t.Errorf("handler ran %d times; a parked served call never runs", n)
				}
				return
			}
			if err != nil || out != "refunded" || ran.Load() != 1 {
				t.Errorf("ServeCall = %q, %v (handler ran %d); want the call served", out, err, ran.Load())
			}
		})
	}
}

// The source is read with the served call's context (metadata and all)
// once per call — ServeCall is the server's "run" of one call.
func TestServeCallReadsParkSourcePerCall(t *testing.T) {
	var reads atomic.Int32
	parkRefund := atomic.Bool{}
	agt := core.New(wefttest.Script(wefttest.Say("unused")),
		core.Tool("refund", "r", func(context.Context, struct{}) (string, error) { return "refunded", nil }),
		core.ParkSource(func(context.Context) []string {
			reads.Add(1)
			if parkRefund.Load() {
				return []string{"refund"}
			}
			return nil
		}))
	call := core.ToolCallPart{ID: "c1", Name: "refund", Args: []byte(`{}`)}
	if _, err := agt.ServeCall(context.Background(), call, ""); err != nil {
		t.Fatalf("first call: %v", err)
	}
	parkRefund.Store(true)
	if _, err := agt.ServeCall(context.Background(), call, ""); !errors.Is(err, core.ErrApprovalRequired) {
		t.Errorf("second call after the breakpoint was set: %v, want ErrApprovalRequired", err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("source read %d times, want once per served call", n)
	}
}
