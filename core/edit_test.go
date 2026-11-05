package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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

// C4: an edited call that parks again (here its outcome is unknown,
// under ParkUnknown) is reported on Pending as the transcript holds it —
// the model's own arguments — so what a person reads on Pending is what
// a plain Approve of it runs. The edit covered one resume; to run it
// again, decide it again with ApproveEdited.
func TestEditedCallParkedAgainReportsTheTranscriptsCall(t *testing.T) {
	type in struct {
		Amount int `json:"amount"`
	}
	var mu sync.Mutex
	var ranWith []int
	release := make(chan struct{})
	pay := core.Tool("pay", "Pay.", func(ctx context.Context, v in) (string, error) {
		mu.Lock()
		ranWith = append(ranWith, v.Amount)
		n := len(ranWith)
		mu.Unlock()
		if n == 1 {
			<-release // answers after its Timeout
		}
		return "paid", nil
	}, core.RequireApproval(), core.Timeout(5*time.Millisecond))
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"amount":1000}`}),
		wefttest.Say("done"),
		wefttest.Say("done"),
	), pay)
	res, err := agent.Generate(context.Background(), core.Prompt("go"))
	if err != nil || len(res.Pending) != 1 {
		t.Fatalf("run 1: %v %+v", err, res)
	}
	res2, err := agent.Generate(context.Background(), core.Messages(res.Messages...),
		core.ApproveEdited("c1", json.RawMessage(`{"amount":10}`), "user"), core.ParkUnknown())
	close(release)
	if err != nil || len(res2.Pending) != 1 {
		t.Fatalf("run 2: %v, pending %+v", err, res2.Pending)
	}
	if got := string(res2.Pending[0].Args); got != `{"amount":1000}` {
		t.Fatalf("Pending[0].Args = %s, want the transcript's {\"amount\":1000}", got)
	}
	if _, err := agent.Generate(context.Background(), core.Messages(res2.Messages...), core.Approve("c1")); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ranWith) != 2 || ranWith[0] != 10 || ranWith[1] != 1000 {
		t.Errorf("ran with %v; want the edit once, then what Pending showed", ranWith)
	}
}

// C7, rule 8: an edited call's result — the EDITED line and the tool's
// output — is capped as one: never past MaxResultBytes (plus the
// marker), the pinned line intact, the marker the model sees present.
func TestEditedResultIsCapped(t *testing.T) {
	pay := core.Tool("pay", "Pay.", func(context.Context, struct {
		Memo string `json:"memo"`
	}) (string, error) {
		return strings.Repeat("x", 100), nil
	}, core.RequireApproval(), core.MaxResultBytes(64))
	agent := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "c1", Args: `{"memo":"a"}`}),
		wefttest.Say("done"),
		wefttest.Say("done"),
	), pay)
	res, err := agent.Generate(context.Background(), core.Prompt("go"))
	if err != nil {
		t.Fatal(err)
	}
	edited := `{"memo":"` + strings.Repeat("y", 4000) + `"}`
	res2, err := agent.Generate(context.Background(), core.Messages(res.Messages...), core.ApproveEdited("c1", json.RawMessage(edited), "user"))
	if err != nil {
		t.Fatal(err)
	}
	var got string
	for _, m := range res2.Messages {
		for _, p := range m.Content {
			if r, ok := p.(core.ToolResultPart); ok && r.CallID == "c1" {
				got = r.Content
			}
		}
	}
	full := "EDITED: the call ran with the arguments " + edited + "\n" + strings.Repeat("x", 100)
	want := full[:64] + fmt.Sprintf("\n…[truncated %d bytes]", len(full)-64)
	if got != want {
		t.Errorf("result = %q\nwant %q", got, want)
	}
}
