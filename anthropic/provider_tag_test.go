package anthropic

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/weftbase/weft/core"
)

// A signature goes back to Anthropic only when Anthropic issued it: a
// part tagged with another provider is dropped like an unsigned one,
// and an untagged signature (a transcript from before the tag) is
// kept.
func TestConvertSignatureProviderTag(t *testing.T) {
	cases := []struct {
		name    string
		part    core.ReasoningPart
		wantSig string // "" = no thinking block sent
	}{
		{"own tag is sent", core.ReasoningPart{Text: "plan", Signature: "sig-a", Provider: "anthropic"}, "sig-a"},
		{"legacy untagged is sent", core.ReasoningPart{Text: "plan", Signature: "sig-old"}, "sig-old"},
		{"gemini-signed is dropped", core.ReasoningPart{Text: "plan", Signature: "c2lnLTE=", Provider: "google"}, ""},
		{"unknown provider is dropped", core.ReasoningPart{Text: "plan", Signature: "sig-x", Provider: "elsewhere"}, ""},
		{"unsigned is dropped", core.ReasoningPart{Text: "plan"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blocks := assistantBlocks(core.Message{Role: core.RoleAssistant, Content: []core.Part{
				tc.part,
				core.TextPart{Text: "hello"},
				// A foreign call signature never reaches the wire either:
				// tool_use has no field for one.
				core.ToolCallPart{ID: "c1", Name: "probe", Args: json.RawMessage(`{"n":1}`), Signature: "c2lnLWM=", Provider: "google"},
			}})
			var got string
			thinking := 0
			for _, b := range blocks {
				if b.OfThinking != nil {
					thinking++
					got = b.OfThinking.Signature
				}
			}
			if got != tc.wantSig || (tc.wantSig == "") != (thinking == 0) {
				t.Errorf("thinking blocks = %d, signature %q; want signature %q", thinking, got, tc.wantSig)
			}
			wire, _ := json.Marshal(blocks)
			if strings.Contains(string(wire), "c2lnLWM=") {
				t.Errorf("the foreign call signature reached the wire: %s", wire)
			}
		})
	}
}

// The signature the stream yields names its issuer.
func TestStreamSignatureCarriesProvider(t *testing.T) {
	evs, err := collect(fixtureModel(t, "thinking_then_tool_use"), basicReq)
	if err != nil {
		t.Fatal(err)
	}
	signed := 0
	for _, ev := range evs {
		if e, ok := ev.(core.ModelReasoningDelta); ok && e.Signature != "" {
			signed++
			if e.Provider != "anthropic" {
				t.Errorf("signed delta provider = %q, want anthropic", e.Provider)
			}
		}
	}
	if signed == 0 {
		t.Fatal("fixture yielded no signed reasoning delta")
	}
}
