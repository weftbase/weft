//go:build unix

// The crash matrix's tool windows (review WR-10 / L9): the session's
// writes around a call with effects, each proved under SIGKILL like
// the rest of the matrix.
//
//	tool_running    killed inside the handler of a ReplayNever call: its
//	                step and dispatch record are on file, its result is
//	                not — a recovery request for a person, the effect
//	                counted once
//	tool_ran_keyed  the same inside a ReplayKeyed call: RecoverTurn
//	                retries it under its same idempotency key, and a
//	                receiver that deduplicates applies it once
//	tool_dispatched killed after the dispatch record, before the handler
//	                started (in handler-side middleware): the record says
//	                "may have run", so it parks for a person, never run
//	                on the session's own say, and no effect happened
//	recover_batch   killed right after RecoverTurn's batch landed: the
//	                recovery is recorded once — a second RecoverTurn has
//	                nothing to do, and the request waits for its decision
//
// The effect is a line in a file beside the storage (<path>.effects),
// synced before the handler goes on: what a system of record holds
// after the process is gone. A torn RecoverTurn batch (a prefix of its
// lines, mid-write) is not reachable with a SIGKILL timed from outside
// the write; thread's TestTornRecoverBatchStaysCutOff proves every
// prefix in process.

package threadtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
	"github.com/weftbase/weft/thread"
)

// CrashMatrixToolID is the tool points' session.
const CrashMatrixToolID = "s_mx_tool"

// toolPoints are the tool windows' points.
var toolPoints = []string{"tool_running", "tool_ran_keyed", "tool_dispatched", "recover_batch"}

func isToolPoint(point string) bool {
	for _, p := range toolPoints {
		if p == point {
			return true
		}
	}
	return false
}

// effectsPath is the file a tool point's effects land in.
func effectsPath(storage string) string { return storage + ".effects" }

// recordEffect appends one line to the effects file and syncs it: the
// effect is in the system of record before the handler goes on.
func recordEffect(path, line string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(line + "\n"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// readEffects returns the effects file's lines (none when it does not
// exist).
func readEffects(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(string(raw))
}

// toolMatrixPay is the tool points' pay tool. Each run records its
// idempotency key as an effect, then: in the child, blocks (the parent
// kills it) or waits for its run to end (recover_batch); in the parent
// it answers.
func toolMatrixPay(point, effects string, child bool) *core.ToolDef {
	opts := []core.ToolOption{}
	if point == "tool_ran_keyed" {
		opts = append(opts, core.Replay(core.ReplayKeyed))
	}
	if point == "tool_dispatched" && child {
		// Handler-side middleware runs past the dispatch gate: the
		// record is on file and synced, the handler has not started.
		opts = append(opts, core.WrapHandler(func(next core.ToolCaller) core.ToolCaller {
			return func(ctx context.Context, call core.ToolCallPart) (string, error) {
				fmt.Println("crashmx:" + point + ":waiting")
				time.Sleep(time.Hour)
				return next(ctx, call)
			}
		}))
	}
	return core.Tool("pay", "Pay.", func(ctx context.Context, _ struct{}) (string, error) {
		call, _ := core.CallFromContext(ctx)
		if err := recordEffect(effects, call.IdempotencyKey); err != nil {
			return "", err
		}
		if !child {
			return "paid", nil
		}
		if point == "recover_batch" {
			<-ctx.Done() // the shutdown fells it: its answer is lost
			return "", ctx.Err()
		}
		fmt.Println("crashmx:" + point + ":waiting")
		time.Sleep(time.Hour)
		return "paid", nil
	}, opts...)
}

// crashMatrixToolChild is the tool points' child side.
func crashMatrixToolChild(point string, st thread.Storage) {
	ctx := context.Background()
	effects := effectsPath(os.Getenv("WEFT_THREADTEST_CRASH_STORAGE"))
	agent := func() *core.Agent {
		return core.New(wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "pay", ID: "call_1", Args: `{}`}),
			wefttest.Say("paid"),
		), toolMatrixPay(point, effects, true))
	}
	s, err := thread.Create(ctx, st, agent(), fixedIDs(CrashMatrixToolID))
	if err != nil {
		helperFail("create failed", err)
	}
	if _, err := s.Send(ctx, core.User("pay")); err != nil {
		helperFail("send failed", err)
	}
	if point != "recover_batch" {
		time.Sleep(time.Hour) // the handler (or its middleware) announces itself; the parent kills
	}
	// recover_batch: a shutdown cuts the turn off while the handler
	// runs, then a fresh Session recovers it, and the process dies
	// right after RecoverTurn's batch landed.
	deadline := time.Now().Add(10 * time.Second)
	for len(readEffectsRaw(effects)) == 0 {
		if time.Now().After(deadline) {
			helperFail("the handler never ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cctx, cancel := context.WithTimeoutCause(ctx, 50*time.Millisecond, thread.ErrShutdown)
	_ = s.Close(cctx)
	cancel()
	if err := s.Close(ctx); err != nil {
		helperFail("close failed", err)
	}
	s2, err := thread.Open(ctx, st, CrashMatrixToolID, agent())
	if err != nil {
		helperFail("open failed", err)
	}
	if _, err := s2.RecoverTurn(ctx); err != nil {
		helperFail("recover failed", err)
	}
	if p := s2.Pending(); len(p) != 1 || !p[0].Recovery {
		helperFail("the recovery parked nothing", p)
	}
	dieAt(point)
}

func readEffectsRaw(path string) string {
	raw, _ := os.ReadFile(path)
	return string(raw)
}

// assertToolCrashPoint holds a tool point's invariant over the reopened
// storage; storage is its location, for the effects file.
func assertToolCrashPoint(t *testing.T, point string, st thread.Storage, storage string) {
	t.Helper()
	ctx := context.Background()
	effects := effectsPath(storage)
	_, entries, _, err := st.Load(ctx, CrashMatrixToolID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dispatched := false
	for _, e := range entries {
		if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Step == thread.StepDispatch && a.CallID == "call_1" {
			dispatched = true
		}
	}
	if !dispatched {
		t.Fatalf("no dispatch record for call_1: kinds %q", kindsOf(entries))
	}
	wantEffects := 1
	if point == "tool_dispatched" {
		wantEffects = 0
	}
	if got := readEffects(t, effects); len(got) != wantEffects {
		t.Fatalf("effects before recovery = %v, want %d", got, wantEffects)
	}
	s, err := thread.Open(ctx, st, CrashMatrixToolID, core.New(&countingModel{}, toolMatrixPay(point, effects, false)))
	if err != nil {
		t.Fatal(err)
	}
	if point == "recover_batch" {
		// The recovery landed once: nothing to recover again, and its
		// request waits for a person.
		if _, err := s.RecoverTurn(ctx); !errors.Is(err, thread.ErrNotPending) {
			t.Fatalf("a second RecoverTurn = %v, want ErrNotPending", err)
		}
	} else {
		if _, err := s.Send(ctx, core.User("did it pay?")); !errors.Is(err, thread.ErrRecoveryRequired) {
			t.Fatalf("Send over the cut-off call = %v, want ErrRecoveryRequired", err)
		}
		rt, err := s.RecoverTurn(ctx)
		if err != nil {
			t.Fatalf("RecoverTurn: %v", err)
		}
		if point == "tool_ran_keyed" {
			// The session's own retry, under the call's same key; a
			// receiver that deduplicates applies it once.
			if rt == nil {
				t.Fatal("RecoverTurn started no keyed retry")
			}
			if _, err := rt.Wait(); err != nil {
				t.Fatalf("the keyed retry: %v", err)
			}
			got := readEffects(t, effects)
			if len(got) != 2 || got[0] != got[1] || got[0] == "" {
				t.Fatalf("effects = %v, want the cut-off attempt and its retry under one key", got)
			}
			closeMatrixSession(t, s)
			continueTurn(t, st, CrashMatrixToolID, -1)
			return
		}
		if rt != nil {
			t.Fatalf("RecoverTurn started a run over a call that may have run: %v", rt.RunID())
		}
	}
	p := s.Pending()
	if len(p) != 1 || p[0].CallID != "call_1" || !p[0].Recovery || !strings.Contains(p[0].Reason, "may have run") {
		t.Fatalf("Pending = %+v, want call_1 as a recovery request", p)
	}
	rt, err := s.Decide(ctx, bound(s, thread.Deny("call_1", "checked by hand")))
	if err != nil || rt == nil {
		t.Fatalf("Decide(deny) = %v, %v", rt, err)
	}
	res, err := rt.Wait()
	if err != nil {
		t.Fatalf("the resume: %v", err)
	}
	for _, m := range res.Messages {
		for _, part := range m.Content {
			if r, ok := part.(core.ToolResultPart); ok && r.CallID == "call_1" && !strings.HasPrefix(r.Content, core.CodeOutcomeUnknown+": ") {
				t.Fatalf("the skipped call reads %q, want the OUTCOME_UNKNOWN skip", r.Content)
			}
		}
	}
	if got := readEffects(t, effects); len(got) != wantEffects {
		t.Fatalf("effects after the skip = %v, want %d", got, wantEffects)
	}
	closeMatrixSession(t, s)
	continueTurn(t, st, CrashMatrixToolID, -1)
}
