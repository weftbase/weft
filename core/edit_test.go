package core_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// ApproveEdited runs a parked call on the decider's arguments, and the
// model's result for it starts with the pinned line naming them (ADR
// 0007, amendment 2026-11-04): the model learns what actually ran.
func TestApproveEditedPinnedPrefix(t *testing.T) {
	var got string
	refund := core.Tool("refund", "Refund.", func(_ context.Context, in struct {
		Amount int `json:"amount"`
	}) (string, error) {
		b, _ := json.Marshal(in)
		got = string(b)
		return "refunded", nil
	}, core.RequireApproval())
	model := wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "c1", Args: `{"amount":900}`}),
		wefttest.Say("done"),
	)
	agent := core.New(model, refund)
	res, err := agent.Generate(context.Background(), core.Prompt("refund"))
	if err != nil || len(res.Pending) != 1 {
		t.Fatalf("park: %v, %+v", err, res)
	}
	res, err = agent.Generate(context.Background(), core.Messages(res.Messages...),
		core.ApproveEdited("c1", json.RawMessage(`{"amount":40}`), "user"))
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"amount":40}` {
		t.Fatalf("the handler ran on %s, want the edited arguments", got)
	}
	want := "EDITED: the call ran with the arguments {\"amount\":40}\nrefunded"
	for _, m := range res.Messages {
		for _, p := range m.Content {
			if r, ok := p.(core.ToolResultPart); ok && r.CallID == "c1" && r.Content != want {
				t.Fatalf("result = %q, want %q", r.Content, want)
			}
		}
	}
}
