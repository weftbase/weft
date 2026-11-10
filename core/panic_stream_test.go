package core_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// panicInfoModel is a model whose Info method — user code the loop
// calls before the run span starts — panics.
type panicInfoModel struct{ core.Model }

func (panicInfoModel) Info() core.ModelInfo { panic("bug:model_info") }

func (m panicInfoModel) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	return m.Model.Stream(ctx, req)
}

// loopPanicCases are the user code the loop goroutine calls with no
// containment of its own: each one panics with value "bug:<name>".
func loopPanicCases() map[string]func() (*core.Agent, []core.RunOption) {
	echo := func() core.Option {
		return core.Tool("echo", "e", func(context.Context, struct{}) (string, error) { return "ok", nil })
	}
	return map[string]func() (*core.Agent, []core.RunOption){
		"prepare_step": func() (*core.Agent, []core.RunOption) {
			return core.New(wefttest.Script(wefttest.Say("x")),
				core.PrepareStep(func(context.Context, int, core.ModelRequest) (core.ModelRequest, error) {
					panic("bug:prepare_step")
				})), nil
		},
		"tool_source": func() (*core.Agent, []core.RunOption) {
			return core.New(wefttest.Script(wefttest.Say("x")),
				core.ToolSource(func() []*core.ToolDef { panic("bug:tool_source") })), nil
		},
		"stop_when": func() (*core.Agent, []core.RunOption) {
			return core.New(wefttest.Script(wefttest.ToolCalls(wefttest.Call{Name: "echo"}), wefttest.Say("x")), echo(),
				core.StopWhen(core.StopFunc(func([]core.StepRecord) bool { panic("bug:stop_when") }))), nil
		},
		"steering": func() (*core.Agent, []core.RunOption) {
			return core.New(wefttest.Script(wefttest.Say("x"))), []core.RunOption{core.Steering(
				func(context.Context, core.SteerPoint) []core.Message { panic("bug:steering") })}
		},
		"model_info": func() (*core.Agent, []core.RunOption) {
			// New reads the agent's own model's Info (a panic there is
			// New's, on the caller's goroutine); a UseModel run's model
			// is first read inside the run, before its span starts.
			return core.New(wefttest.Script(wefttest.Say("x"))),
				[]core.RunOption{core.UseModel(panicInfoModel{wefttest.Script(wefttest.Say("x"))})}
		},
	}
}

// checkRunPanic asserts err is the *RunError a contained loop panic
// becomes: ErrRunPanicked reachable, the *PanicError carrying the value
// and the stack, and the partial transcript on Result.
func checkRunPanic(t *testing.T, name string, err error) *core.RunError {
	t.Helper()
	if !errors.Is(err, core.ErrRunPanicked) {
		t.Fatalf("err = %v, want ErrRunPanicked reachable", err)
	}
	var re *core.RunError
	if !errors.As(err, &re) || re.Result == nil {
		t.Fatalf("err = %#v, want a *RunError with a partial result", err)
	}
	var pe *core.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want a *PanicError reachable", err)
	}
	if pe.Value != "bug:"+name {
		t.Errorf("PanicError.Value = %v, want %q", pe.Value, "bug:"+name)
	}
	if len(pe.Stack) == 0 || !strings.Contains(string(pe.Stack), "panic_stream_test.go") {
		t.Errorf("PanicError.Stack does not show the panicking frame:\n%s", pe.Stack)
	}
	if !strings.Contains(err.Error(), "bug:"+name) {
		t.Errorf("err text %q does not name the panic value", err.Error())
	}
	return re
}

// S1: a panic in user code on the loop goroutine — PrepareStep, a
// ToolSource read, a StopWhen condition, a SteerFunc, a model's Info —
// is contained under Stream: the stream's final element is a *RunError
// matching ErrRunPanicked, never a crash of the process (the producer
// runs on its own goroutine, where nothing of the caller's can recover).
func TestStreamContainsLoopPanic(t *testing.T) {
	for name, mk := range loopPanicCases() {
		t.Run(name, func(t *testing.T) {
			agt, opts := mk()
			var last error
			var sawFinish bool
			n := 0
			for ev, err := range agt.Stream(context.Background(), append(opts, core.Prompt("hi"))...).Events() {
				n++
				if err != nil {
					last = err
					continue
				}
				if _, ok := ev.(core.RunFinish); ok {
					sawFinish = true
				}
			}
			if sawFinish {
				t.Error("a panicked run delivered RunFinish")
			}
			re := checkRunPanic(t, name, last)
			if name == "stop_when" {
				// The panic came after the tool batch: the partial
				// transcript keeps the call and its result.
				if got := len(re.Result.Messages); got != 3 {
					t.Errorf("partial transcript has %d messages, want prompt, call, result", got)
				}
				if re.Step != 1 {
					t.Errorf("RunError.Step = %d, want 1 (the step after the completed one)", re.Step)
				}
			}
		})
	}
}

// Generate and Stream agree (same code, same outcome): the contained
// panic is Generate's returned *RunError too, and the caller needs no
// recover of its own.
func TestGenerateContainsLoopPanic(t *testing.T) {
	for name, mk := range loopPanicCases() {
		t.Run(name, func(t *testing.T) {
			agt, opts := mk()
			res, err := agt.Generate(context.Background(), append(opts, core.Prompt("hi"))...)
			if res != nil {
				t.Errorf("res = %v, want nil beside the run error", res)
			}
			checkRunPanic(t, name, err)
		})
	}
}

// A contained panic is the run's outcome: OnRunEnd sees it once, with
// the same *RunError the caller receives — once the run span had
// started (a panic before it, a model's Info, is no run that ended).
func TestOnRunEndSeesContainedPanic(t *testing.T) {
	var got []error
	agt := core.New(wefttest.Script(wefttest.Say("ok")),
		core.PrepareStep(func(context.Context, int, core.ModelRequest) (core.ModelRequest, error) {
			panic("bug:prepare_step")
		}),
		core.OnRunEnd(func(_ context.Context, _ *core.RunResult, err error) { got = append(got, err) }))
	_, err := agt.Stream(context.Background(), core.Prompt("hi")).Wait()
	checkRunPanic(t, "prepare_step", err)
	if len(got) != 1 || got[0] != err {
		t.Errorf("OnRunEnd saw %v, want exactly the run's error %v", got, err)
	}
}

// A Subagent child's loop panic is the child's run error, which the
// parent reads as the delegating tool's error result — the parent run
// goes on.
func TestSubagentChildLoopPanicIsToolError(t *testing.T) {
	child := core.New(wefttest.Script(wefttest.Say("never")), core.Name("child"),
		core.ToolSource(func() []*core.ToolDef { panic("bug:child") }))
	parent := core.New(wefttest.Script(wefttest.ToolCalls(wefttest.Call{Name: "delegate"}), wefttest.Say("parent done")),
		core.Subagent("delegate", "Runs the child.", child))
	var finish *core.ToolFinish
	for ev, err := range parent.Stream(context.Background(), core.Prompt("go")).Events() {
		if err != nil {
			t.Fatalf("parent run failed: %v", err)
		}
		if f, ok := ev.(core.ToolFinish); ok {
			finish = &f
		}
	}
	if finish == nil || !finish.IsError || !strings.Contains(finish.Content, "run panicked: bug:child") {
		t.Errorf("delegate's result = %+v, want an error result naming the child's panic", finish)
	}
}
