package core_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// CurrentTools resolves a ToolSource fresh on every call, validates the
// snapshot as a step does, and returns copies; without a source it is
// the registered set.
func TestCurrentToolsResolvesTheToolSource(t *testing.T) {
	echo := func(name string) *core.ToolDef {
		return core.Tool(name, "Echo.", func(_ context.Context, in struct{ S string }) (string, error) { return in.S, nil },
			core.RequireApproval())
	}
	static := core.New(wefttest.Script(), echo("a"), echo("b"))
	got, err := static.CurrentTools()
	if err != nil || len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("static CurrentTools = %v, %v", got, err)
	}

	var set []*core.ToolDef
	src := core.New(wefttest.Script(), echo("static"), core.ToolSource(func() []*core.ToolDef { return set }))
	set = []*core.ToolDef{echo("x")}
	got, err = src.CurrentTools()
	if err != nil || len(got) != 1 || got[0].Name != "x" || !got[0].RequiresApproval() {
		t.Fatalf("source CurrentTools = %v, %v", got, err)
	}
	if len(src.Tools()) != 1 || src.Tools()[0].Name != "static" {
		t.Fatal("Tools must keep reporting the static set")
	}
	got[0].Name = "mutated"
	set = append(set, echo("y"))
	got, err = src.CurrentTools()
	if err != nil || len(got) != 2 || got[0].Name != "x" {
		t.Fatalf("a fresh fetch per call, copies out: %v, %v", got, err)
	}

	set = []*core.ToolDef{echo("x"), echo("x")}
	if _, err := src.CurrentTools(); !errors.Is(err, core.ErrDuplicateTool) {
		t.Fatalf("duplicate snapshot = %v, want ErrDuplicateTool", err)
	}
	set = []*core.ToolDef{nil}
	if _, err := src.CurrentTools(); !errors.Is(err, core.ErrNilTool) {
		t.Fatalf("nil snapshot = %v, want ErrNilTool", err)
	}
}

// CurrentTools shows what a ToolSource holds now, which Tools leaves out.
func ExampleAgent_CurrentTools() {
	registry := []*core.ToolDef{
		core.Tool("lookup", "Look up.", func(_ context.Context, in struct{ ID string }) (string, error) { return in.ID, nil }),
	}
	agt := core.New(wefttest.Script(), core.ToolSource(func() []*core.ToolDef { return registry }))
	tools, err := agt.CurrentTools()
	if err != nil {
		panic(err)
	}
	fmt.Println(len(agt.Tools()), tools[0].Name)
	// Output: 0 lookup
}
