package google

import (
	"encoding/json"
	"testing"

	"github.com/weftbase/weft/core"
)

// A thought signature goes back to Gemini only when Gemini issued it:
// a part tagged with another provider is treated as unsigned, and an
// untagged signature (a transcript from before the tag) is kept.
func TestConvertSignatureProviderTag(t *testing.T) {
	own := encodeSignature([]byte("sig-g"))
	call := func(sig, prov string) core.ToolCallPart {
		return core.ToolCallPart{ID: "a", Name: "probe", Args: json.RawMessage(`{"n":1}`), Signature: sig, Provider: prov}
	}
	cases := []struct {
		name    string
		content []core.Part
		// want, per sent part: "thought:<sig>", "call:<sig>", "text"
		want []string
	}{
		{
			"own reasoning is sent",
			[]core.Part{core.ReasoningPart{Text: "plan", Signature: own, Provider: "google"}, core.TextPart{Text: "hi"}},
			[]string{"thought:sig-g", "text"},
		},
		{
			"legacy untagged reasoning is sent",
			[]core.Part{core.ReasoningPart{Text: "plan", Signature: own}, core.TextPart{Text: "hi"}},
			[]string{"thought:sig-g", "text"},
		},
		{
			"anthropic-signed reasoning is dropped",
			[]core.Part{core.ReasoningPart{Text: "plan", Signature: "ErUBCkYIBxgC", Provider: "anthropic"}, core.TextPart{Text: "hi"}},
			[]string{"text"},
		},
		{
			"own call signature is sent",
			[]core.Part{call(own, "google")},
			[]string{"call:sig-g"},
		},
		{
			"legacy untagged call signature is sent",
			[]core.Part{call(own, "")},
			[]string{"call:sig-g"},
		},
		{
			"foreign call signature is dropped",
			[]core.Part{call("sig-foreign", "anthropic")},
			[]string{"call:"},
		},
		{
			"legacy step signature falls back to the first call",
			[]core.Part{core.ReasoningPart{Text: "plan", Signature: own}, call("", "")},
			[]string{"call:sig-g"},
		},
		{
			"a foreign step signature does not fall back to a call",
			[]core.Part{core.ReasoningPart{Text: "plan", Signature: "ErUBCkYIBxgC", Provider: "anthropic"}, call("", "")},
			[]string{"call:"},
		},
		{
			"a foreign call signature does not hide the legacy fallback's absence",
			[]core.Part{core.ReasoningPart{Text: "plan", Signature: "ErUBCkYIBxgC", Provider: "anthropic"}, call("sig-foreign", "anthropic")},
			[]string{"call:"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, p := range modelParts(core.Message{Role: core.RoleAssistant, Content: tc.content}) {
				switch {
				case p.FunctionCall != nil:
					got = append(got, "call:"+string(p.ThoughtSignature))
				case p.Thought:
					got = append(got, "thought:"+string(p.ThoughtSignature))
				default:
					got = append(got, "text")
				}
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parts = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("parts = %v, want %v", got, tc.want)
					break
				}
			}
		})
	}
}

// Every signature the stream yields — on a call or on a reasoning
// delta — names its issuer.
func TestStreamSignatureCarriesProvider(t *testing.T) {
	for _, fx := range []string{"signature_on_call", "signature_per_call", "reasoning_passthrough"} {
		evs, err := collect(fixtureModel(t, fx), basicReq)
		if err != nil {
			t.Fatal(err)
		}
		signed := 0
		for _, ev := range evs {
			switch e := ev.(type) {
			case core.ModelReasoningDelta:
				if e.Signature != "" {
					signed++
					if e.Provider != "google" {
						t.Errorf("%s: signed delta provider = %q, want google", fx, e.Provider)
					}
				}
			case core.ModelToolCall:
				if e.Signature != "" {
					signed++
					if e.Provider != "google" {
						t.Errorf("%s: signed call provider = %q, want google", fx, e.Provider)
					}
				}
			}
		}
		if signed == 0 {
			t.Errorf("%s: fixture yielded no signature", fx)
		}
	}
}
