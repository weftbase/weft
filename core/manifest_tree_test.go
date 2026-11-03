package core_test

import (
	"bytes"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// An agent's manifest — and so weft.manifest.hash, its sha256 — pins
// the agents it delegates to: a Subagent tool carries its child's own
// hash, which pins the child's children in turn. A change anywhere in
// the delegation tree (a grandchild's prompt, a child's model or
// tools) is a new version of every agent above it, while the manifest
// still describes only the agents it was given (ADR 0012). An unnamed
// child is pinned too.
func TestManifestPinsTheDelegationTree(t *testing.T) {
	type variant struct {
		grandPrompt, childModel string
		childTool               bool
		named                   bool
	}
	build := func(v variant) []byte {
		t.Helper()
		grand := core.New(wefttest.Script(), core.Name("grand"), core.Instructions(v.grandPrompt))
		opts := []core.Option{core.Instructions("child"),
			core.Subagent("ask_grand", "Ask the grandchild.", grand)}
		if v.named {
			opts = append(opts, core.Name("child"))
		}
		if v.childTool {
			opts = append(opts, lookupTool())
		}
		child := core.New(wefttest.Script(wefttest.Say(v.childModel)), opts...)
		parent := core.New(wefttest.Script(), core.Name("parent"),
			core.Subagent("ask_child", "Ask the child.", child))
		b, err := core.Manifest(parent)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	base := variant{grandPrompt: "v1", childModel: "m1", named: true}
	same := build(base)
	if again := build(base); !bytes.Equal(same, again) {
		t.Fatalf("the same tree renders two manifests:\n%s\n%s", same, again)
	}
	for name, v := range map[string]variant{
		"grandchild prompt": {grandPrompt: "v2", childModel: "m1", named: true},
		"child tools":       {grandPrompt: "v1", childModel: "m1", childTool: true, named: true},
		"unnamed child":     {grandPrompt: "v1", childModel: "m1", named: false},
	} {
		if bytes.Equal(build(v), same) {
			t.Errorf("%s changed, the parent's manifest did not", name)
		}
	}
	unnamed := variant{grandPrompt: "v1", childModel: "m1"}
	changed := unnamed
	changed.grandPrompt = "v2"
	if bytes.Equal(build(unnamed), build(changed)) {
		t.Error("a grandchild under an unnamed child changed, the parent's manifest did not")
	}
}
