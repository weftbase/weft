package mcp

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// S2: a tool the agent's ParkSource names — or any tool, when the
// source panics — answers over Serve with the ErrApprovalRequired text,
// never running (ADR 0030, amendment 2026-11-10).
func TestServeHonoursParkSource(t *testing.T) {
	for name, src := range map[string]func(context.Context) []string{
		"names_the_tool": func(context.Context) []string { return []string{"refund"} },
		"panics":         func(context.Context) []string { panic("breakpoint store down") },
	} {
		t.Run(name, func(t *testing.T) {
			var ran atomic.Int32
			agt := core.New(wefttest.Script(), core.Name("bp"), core.ParkSource(src),
				core.Tool("refund", "Refund.", func(context.Context, struct{}) (string, error) {
					ran.Add(1)
					return "refunded", nil
				}))
			s := sdk.NewServer(&sdk.Implementation{Name: "srv", Version: "0"}, nil)
			Serve(s, agt, "Breakpointed agent.")
			res, err := newSession(t, s).CallTool(context.Background(), &sdk.CallToolParams{Name: "refund"})
			if err != nil {
				t.Fatal(err)
			}
			if !res.IsError || ran.Load() != 0 {
				t.Fatalf("parked tool ran over MCP: %+v (handler ran %d)", res, ran.Load())
			}
			if got := res.Content[0].(*sdk.TextContent).Text; !strings.Contains(got, "weft: tool call requires approval") {
				t.Errorf("result text = %q, want the approval refusal", got)
			}
		})
	}
}
