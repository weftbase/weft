package core_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/weftbase/weft/core"
)

// SettingsOf writes down what a run is started under — its data run
// options and the values its context carries — and Options puts it
// back: the settings of a run restored from the JSON are the settings
// of the run they were taken from. A caller restoring a parked or
// queued run after a restart (weft/thread) depends on the round trip.
func TestSettingsOfRoundTrip(t *testing.T) {
	ctx := core.WithMetadata(context.Background(), map[string]string{"tenant": "acme", "drop": "x"})
	temp, maxTok := 0.3, 128
	opts := []core.RunOption{
		core.ParkOn("refund"),
		core.ParkAllExcept("lookup", "refund"),
		core.OnlyTools("refund", "lookup", "refund"),
		core.MaxSteps(4),
		core.Parallelism(2),
		core.Instructions("be careful"),
		core.Thinking(core.ThinkingConfig{Level: core.ThinkLow}),
		core.ToolChoice(core.ToolChoiceConfig{Mode: core.ToolChoiceNone}),
		core.Params(core.RequestParams{Temperature: &temp, MaxTokens: &maxTok, Stop: []string{"END"}}),
		core.Metadata(map[string]string{"order": "7", "drop": ""}),
		core.UseModel(nil), // not data: skipped
	}
	s := core.SettingsOf(ctx, opts...)
	if s == nil {
		t.Fatal("SettingsOf = nil")
	}
	want := &core.RunSettings{
		Park:         &core.ParkRule{On: []string{"refund"}, All: true, AllExcept: []string{"lookup", "refund"}},
		OnlyTools:    []string{"lookup", "refund"},
		MaxSteps:     4,
		Parallelism:  2,
		Instructions: s.Instructions,
		Thinking:     &core.ThinkingConfig{Level: core.ThinkLow},
		ToolChoice:   &core.ToolChoiceConfig{Mode: core.ToolChoiceNone},
		Params:       s.Params,
		Metadata:     map[string]string{"tenant": "acme", "order": "7", "drop": ""},
	}
	if !reflect.DeepEqual(s, want) || *s.Instructions != "be careful" || *s.Params.Temperature != temp {
		t.Fatalf("SettingsOf = %+v\nwant %+v", s, want)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var back core.RunSettings
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if again := core.SettingsOf(context.Background(), back.Options()...); !reflect.DeepEqual(again, s) {
		t.Fatalf("restored settings = %+v\nwant %+v", again, s)
	}
	if c := s.Clone(); !reflect.DeepEqual(c, s) || c.Params == s.Params || &c.OnlyTools[0] == &s.OnlyTools[0] {
		t.Fatalf("Clone shares or differs: %+v", c)
	}
	if core.SettingsOf(context.Background(), core.UseModel(nil), core.Prompt("hi")) != nil {
		t.Fatal("SettingsOf over options that are not data should be nil")
	}
	if (*core.RunSettings)(nil).Options() != nil {
		t.Fatal("nil RunSettings has options")
	}
}
