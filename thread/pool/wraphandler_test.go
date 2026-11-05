package pool_test

import (
	"strings"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread/pool"
)

// Handler middleware handed to a wrap through ToolOptions would run on
// the bare path only — under a session the delegation answers the call
// in place of the subagent's handler — so Wrap refuses it, directly or
// inside a core.ToolOptions bundle, rather than skip it silently.
// WrapTools middleware, which runs above the delegation, is accepted.
func TestWrapRefusesHandlerMiddleware(t *testing.T) {
	mw := func(next core.ToolCaller) core.ToolCaller { return next }
	child := core.New(wefttest.Script(wefttest.Say("ok")))
	for _, tc := range []struct {
		name string
		opt  core.ToolOption
		bad  bool
	}{
		{"WrapHandler", core.WrapHandler(mw), true},
		{"bundled WrapHandler", core.ToolOptions(core.Timeout(0), core.ToolOptions(core.WrapHandler(mw))), true},
		{"empty WrapHandler", core.WrapHandler(nil), false},
		{"WrapTools", core.WrapTools(mw), false},
		{"Timeout", core.Timeout(0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pool.New(1)
			_, err := p.Wrap("worker", "w", child, pool.ToolOptions(tc.opt))
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "WrapHandler") {
					t.Fatalf("Wrap = %v, want a refusal naming WrapHandler", err)
				}
				defer func() {
					if recover() == nil {
						t.Error("MustWrap did not panic")
					}
				}()
				p.MustWrap("worker2", "w", child, pool.ToolOptions(tc.opt))
				return
			}
			if err != nil {
				t.Fatalf("Wrap = %v, want accepted", err)
			}
		})
	}
}
