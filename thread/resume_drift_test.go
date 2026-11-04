package thread_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/wefttest"
)

// A parked call resumed on another build (a deploy, a restart onto new
// code) runs on its approval only when its tool is the one it parked
// under: a tool since removed, renamed, or given another input schema
// — even one the old arguments still decode into — is denied with a
// model-visible reason, never run, and the audit trail names the
// drift. A change to the approval mark alone moves nothing: an
// approved call runs exactly once, a denied one never.
func TestResumeAfterCodeDrift(t *testing.T) {
	type amountInput struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount,omitempty"`
	}
	type rebuilt struct {
		tools    func(ran *atomic.Int32) []core.Option
		approved string // the result an approved call must end with: "" = ran
		oldMark  bool   // the parking build's tool had RequireApproval
	}
	refundOK := func(ran *atomic.Int32) func(context.Context, refundInput) (string, error) {
		return func(ctx context.Context, in refundInput) (string, error) {
			if c, _ := core.CallFromContext(ctx); c.Approved {
				ran.Add(1)
			} else {
				ran.Add(100) // ran without an approval
			}
			return "refunded", nil
		}
	}
	cases := map[string]rebuilt{
		"removed": {tools: func(*atomic.Int32) []core.Option { return nil },
			approved: `DENIED: tool "refund" is no longer available; the approved call was not run`},
		"renamed": {tools: func(ran *atomic.Int32) []core.Option {
			return []core.Option{core.Tool("refund_order", "Refund.", refundOK(ran), core.RequireApproval())}
		}, approved: `DENIED: tool "refund" is no longer available; the approved call was not run`},
		"schema changed": {tools: func(ran *atomic.Int32) []core.Option {
			return []core.Option{core.Tool("refund", "Refund.", func(ctx context.Context, in amountInput) (string, error) {
				ran.Add(1)
				return "refunded", nil
			}, core.RequireApproval())}
		}, approved: `DENIED: tool "refund" changed its input since this call was approved; the call was not run — call it again if it is still wanted`},
		"approval mark removed": {oldMark: true, tools: func(ran *atomic.Int32) []core.Option {
			return []core.Option{core.Tool("refund", "Refund.", refundOK(ran))}
		}},
		"approval mark added": {tools: func(ran *atomic.Int32) []core.Option {
			return []core.Option{core.Tool("refund", "Refund.", refundOK(ran), core.RequireApproval())}
		}},
	}
	for name, c := range cases {
		for _, approve := range []bool{true, false} {
			t.Run(name+map[bool]string{true: "/approved", false: "/denied"}[approve], func(t *testing.T) {
				ctx := context.Background()
				st, err := jsonl.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				var ran atomic.Int32
				// Parked by the tool's own mark, or by ParkOn when the
				// parking build's tool has none: the mark being added or
				// removed later is drift the approval must survive.
				var mark []core.ToolOption
				if c.oldMark {
					mark = append(mark, core.RequireApproval())
				}
				old := core.New(wefttest.Script(wefttest.ToolCalls(wefttest.Call{Name: "refund", ID: "call_1", Args: `{"order_id":"1"}`})),
					core.Name("drift"), core.Tool("refund", "Refund.", refundOK(&ran), mark...))
				s, err := thread.Create(ctx, st, old)
				if err != nil {
					t.Fatal(err)
				}
				turn, err := s.Send(ctx, core.User("refund"), thread.RunOptions(core.ParkOn("refund")))
				if err != nil {
					t.Fatal(err)
				}
				if res, err := turn.Wait(); err != nil || len(res.Pending) != 1 {
					t.Fatalf("park: %v %+v", err, res)
				}
				abandon(t, st, s.ID())

				opts := append([]core.Option{core.Name("drift")}, c.tools(&ran)...)
				s2, err := thread.Open(ctx, st, s.ID(), core.New(wefttest.Script(wefttest.Say("done")), opts...))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s2.Close(context.Background()) })
				d := thread.Deny("call_1", "no")
				if approve {
					d = thread.Approve("call_1")
				}
				rt, err := s2.Decide(ctx, bound(s2, d))
				if err != nil {
					t.Fatal(err)
				}
				res, err := rt.Wait()
				if err != nil {
					t.Fatal(err)
				}
				var result string
				for _, m := range res.Messages {
					for _, p := range m.Content {
						if r, ok := p.(core.ToolResultPart); ok && r.CallID == "call_1" {
							result = r.Content
						}
					}
				}
				drifted := false
				for _, e := range s2.Audit() {
					if a, ok := e.(thread.ApprovalAuditEntry); ok && a.Outcome == "drift" && a.CallID == "call_1" {
						drifted = true
					}
				}
				switch {
				case !approve:
					if ran.Load() != 0 || result != "DENIED: no" || drifted {
						t.Fatalf("denied call: ran %d, result %q, drift audited %v", ran.Load(), result, drifted)
					}
				case c.approved == "":
					if ran.Load() != 1 || result != "refunded" || drifted {
						t.Fatalf("approved call: ran %d (want once, approved), result %q, drift audited %v", ran.Load(), result, drifted)
					}
				default:
					if ran.Load() != 0 || result != c.approved || !drifted {
						t.Fatalf("drifted call: ran %d (want never), result %q, drift audited %v", ran.Load(), result, drifted)
					}
					if !strings.HasPrefix(result, "DENIED: ") {
						t.Fatalf("result %q is not a model-visible denial", result)
					}
				}
			})
		}
	}
}
