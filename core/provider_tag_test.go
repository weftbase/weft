package core_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/weftbase/weft/core"
)

// The provider that issued a signature rides it from the model event
// to the transcript part and through the message wire, additively: a
// part without a signature carries no tag and encodes as before.
func TestSignatureProviderTagRoundTrip(t *testing.T) {
	model := &turnModel{turns: [][]core.ModelEvent{
		{
			core.ModelReasoningDelta{Text: "plan"},
			core.ModelReasoningDelta{Signature: "sig-r", Provider: "google"},
			core.ModelReasoningDelta{Text: "unsigned", Provider: "google"},
			core.ModelToolCall{ID: "c1", Name: "t", Args: json.RawMessage(`{}`), Signature: "sig-c", Provider: "google"},
			core.ModelToolCall{ID: "c2", Name: "t", Args: json.RawMessage(`{}`), Provider: "google"},
			core.ModelFinish{Reason: core.StopToolCalls, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}},
		},
		{
			core.ModelTextDelta{Text: "done"},
			core.ModelFinish{Reason: core.StopEndTurn, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}},
		},
	}}
	res, err := core.New(model, core.Tool("t", "", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	})).Generate(context.Background(), core.Prompt("x"))
	if err != nil {
		t.Fatal(err)
	}
	want := []core.Part{
		core.ReasoningPart{Text: "plan", Signature: "sig-r", Provider: "google"},
		// The tag names who issued the signature: no signature, no tag.
		core.ReasoningPart{Text: "unsigned"},
		core.ToolCallPart{ID: "c1", Name: "t", Args: json.RawMessage(`{}`), Signature: "sig-c", Provider: "google"},
		core.ToolCallPart{ID: "c2", Name: "t", Args: json.RawMessage(`{}`)},
	}
	if got := res.Messages[1].Content; !reflect.DeepEqual(got, want) {
		t.Fatalf("assistant parts:\n got  %+v\n want %+v", got, want)
	}
	b, err := json.Marshal(res.Messages[1])
	if err != nil {
		t.Fatal(err)
	}
	wantWire := `{"role":"assistant","content":[` +
		`{"type":"reasoning","text":"plan","signature":"sig-r","provider":"google"},` +
		`{"type":"reasoning","text":"unsigned"},` +
		`{"type":"tool_call","id":"c1","name":"t","args":{},"signature":"sig-c","provider":"google"},` +
		`{"type":"tool_call","id":"c2","name":"t","args":{}}]}`
	if string(b) != wantWire {
		t.Errorf("wire format:\n got  %s\n want %s", b, wantWire)
	}
	var back core.Message
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Content, want) {
		t.Errorf("decoded parts:\n got  %+v\n want %+v", back.Content, want)
	}
	// A transcript written before the tag existed decodes untagged.
	var legacy core.Message
	if err := json.Unmarshal([]byte(`{"role":"assistant","content":[{"type":"reasoning","text":"p","signature":"s"}]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if r := legacy.Content[0].(core.ReasoningPart); r.Provider != "" || r.Signature != "s" {
		t.Errorf("legacy part = %+v, want the signature and no tag", r)
	}
}
