package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// WrapHandler's middleware runs where the handler runs: past the
// approval boundary and the dispatch gate. A call the run parks
// (RequireApproval, ParkOn, ParkSource) or the gate refuses never
// reaches it; one allowed to start does, and may answer in place of
// the handler.
func TestWrapHandlerRunsPastApprovalAndGate(t *testing.T) {
	cases := []struct {
		name     string
		toolOpts []core.ToolOption
		agent    []core.Option
		run      []core.RunOption
		reached  bool
	}{
		{name: "plain", reached: true},
		{name: "RequireApproval", toolOpts: []core.ToolOption{core.RequireApproval()}},
		{name: "ParkOn", run: []core.RunOption{core.ParkOn("act")}},
		{name: "ParkSource", agent: []core.Option{core.ParkSource(func(context.Context) []string { return []string{"act"} })}},
		{name: "gate refuses", run: []core.RunOption{core.BeforeDispatch(func(context.Context, core.Call, *core.ToolDef) error {
			return errors.New("not now")
		})}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var wrapped, handled atomic.Int32
			hmw := func(next core.ToolCaller) core.ToolCaller {
				return func(ctx context.Context, call core.ToolCallPart) (string, error) {
					wrapped.Add(1)
					return "answered by the wrap", nil // in place of next
				}
			}
			act := core.Tool("act", "Act.", func(context.Context, struct{}) (string, error) {
				handled.Add(1)
				return "handled", nil
			}, append(c.toolOpts, core.WrapHandler(hmw))...)
			agent := core.New(wefttest.Script(
				wefttest.ToolCalls(wefttest.Call{Name: "act", ID: "c1", Args: `{}`}),
				wefttest.Say("ok"),
			), append([]core.Option{act}, c.agent...)...)
			res, err := agent.Generate(context.Background(), append([]core.RunOption{core.Prompt("go")}, c.run...)...)
			if err != nil {
				t.Fatal(err)
			}
			if handled.Load() != 0 {
				t.Fatalf("the handler ran: the wrap answers in its place")
			}
			if got := wrapped.Load() == 1; got != c.reached {
				t.Fatalf("handler middleware reached = %v, want %v", got, c.reached)
			}
			if c.reached && !strings.Contains(transcriptOf(res), "answered by the wrap") {
				t.Fatalf("the wrap's answer is not the call's result")
			}
		})
	}
}

// The handler middleware nests inside the tool's WrapTools chain and
// passes on to the handler when it calls next; the manifest lists it
// after the WrapTools middleware.
func TestWrapHandlerOrderAndManifest(t *testing.T) {
	var order []string
	// Two functions, not one closure twice: NameToolMiddleware names a
	// function's code.
	outer := func(next core.ToolCaller) core.ToolCaller {
		return func(ctx context.Context, call core.ToolCallPart) (string, error) {
			order = append(order, "outer")
			return next(ctx, call)
		}
	}
	inner := func(next core.ToolCaller) core.ToolCaller {
		return func(ctx context.Context, call core.ToolCallPart) (string, error) {
			order = append(order, "inner")
			return next(ctx, call)
		}
	}
	act := core.Tool("act", "Act.", func(context.Context, struct{}) (string, error) {
		order = append(order, "handler")
		return "handled", nil
	}, core.WrapHandler(core.NameToolMiddleware("inner", inner)), core.WrapTools(core.NameToolMiddleware("outer", outer)))
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "act", ID: "c1", Args: `{}`}),
		wefttest.Say("ok"),
	), act, core.Name("a"))
	if _, err := agent.Generate(context.Background(), core.Prompt("go")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "outer,inner,handler" {
		t.Fatalf("order = %s, want outer,inner,handler", got)
	}
	b, err := core.Manifest(agent)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Agents []struct {
			Tools []struct {
				Middleware []core.MiddlewareInfo `json:"middleware"`
			} `json:"tools"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	m := doc.Agents[0].Tools[0].Middleware
	if len(m) != 2 || m[0].Name != "outer" || m[1].Name != "inner" {
		t.Fatalf("manifest middleware = %+v, want outer then inner", m)
	}
}

func transcriptOf(res *core.RunResult) string {
	var b strings.Builder
	for _, m := range res.Messages {
		for _, p := range m.Content {
			if r, ok := p.(core.ToolResultPart); ok {
				b.WriteString(r.Content)
			}
		}
	}
	return b.String()
}
