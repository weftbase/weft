package thread_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// The format-6 goldens (ADR 0032): a cut-off turn and a recovery
// request are written with "v":6, so a reader from before recovery
// fails loudly (ErrNewerFormat) on a session whose cut-off calls wait
// for a decision — it would answer them with a synthetic result or run
// one that may have run. Every other turn and request keeps its old
// version. Regenerate with `go test ./thread -update`.
func TestRecoveryFormatGoldens(t *testing.T) {
	ts := at(60)
	call := core.ToolCallPart{ID: "call_1", Name: "charge", Args: json.RawMessage(`{"order_id":"1"}`)}
	cases := []struct {
		file string
		e    thread.Entry
		v    string
	}{
		{"turn_cut_off.json", thread.TurnEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6A", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C5Z", Created: ts,
			RunID: "s_1-t1", Err: "thread: turn cut off: the session's writer stopped before the turn ended", Pending: []core.ToolCallPart{call}, CutOff: true}, `"type":"turn","v":6,`},
		{"approval_request_recovery.json", thread.ApprovalRequestEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6B", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C6A", Created: ts,
			CallID: "call_1", Tool: "charge", Args: json.RawMessage(`{"order_id":"1"}`),
			ArgsSHA256: "f505666b8690e04ec1b2fc27dd7389ae0adb9f8c9fe7ec141b5b1f47818a0d7e", RunID: "s_1-t1",
			Reason: "cut off: the call may have run; approve to retry it with the same idempotency key, deny to skip it", Recovery: true}, `"type":"approval_request","v":6,`},
		// The edit (G2): the edit decision and its revision carry "v":6,
		// so a 0.21 reader — which reads the decision and request kinds
		// at "v":2 and would fold "edit" as no decision, then run the
		// model's arguments on a later approval — refuses the session.
		{"approval_decision_edit.json", thread.ApprovalDecisionEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6C", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C6B", Created: ts,
			CallID: "call_1", Outcome: thread.OutcomeEdit, Who: "bob", Via: "user", RunID: "s_1-t1", RequestID: "e_01J8X9M2K7QW4R5N8T6V2B3C6B",
			ArgsSHA256:       "f505666b8690e04ec1b2fc27dd7389ae0adb9f8c9fe7ec141b5b1f47818a0d7e",
			EditedArgs:       json.RawMessage(`{"order_id":"2"}`),
			EditedArgsSHA256: "0f6d1e5c6a9f0e1d3f2a9fd0e5c8b74d5a4e6f8f2b0c4b3d0a8b6c1e9d2f7a3b"}, `"type":"approval_decision","v":6,`},
		{"approval_request_revision.json", thread.ApprovalRequestEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6D", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C6C", Created: ts,
			CallID: "call_1", Tool: "charge", Args: json.RawMessage(`{"order_id":"2"}`),
			ArgsSHA256: "0f6d1e5c6a9f0e1d3f2a9fd0e5c8b74d5a4e6f8f2b0c4b3d0a8b6c1e9d2f7a3b", RunID: "s_1-t1",
			Reason: "tool requires approval", RevisionOf: "e_01J8X9M2K7QW4R5N8T6V2B3C6B"}, `"type":"approval_request","v":6,`},
		{"approval_decision_cosigners.json", thread.ApprovalDecisionEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6E", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C6D", Created: ts,
			CallID: "call_1", Outcome: thread.OutcomeApprove, Who: "weftbase", Via: "signed", RunID: "s_1-t1", RequestID: "e_01J8X9M2K7QW4R5N8T6V2B3C6D",
			Cosigners: []string{"usr_alice", "usr_carol"}}, `"type":"approval_decision","v":2,`},
		// Dispatch records (ADR 0032): a step that records its
		// dispatches and each dispatch record carry "v":6, so a 0.21
		// reader — which would answer a dangling call "interrupted" and
		// let the model ask for it again — refuses a session whose call
		// may have started.
		{"message_dispatch_recorded.json", thread.MessageEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6F", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C5Z", Created: ts,
			Message: core.Message{Role: core.RoleAssistant, Content: []core.Part{call}}, DispatchRecorded: true}, `"type":"message","v":6,`},
		{"approval_audit_dispatch.json", thread.ApprovalAuditEntry{ID: "e_01J8X9M2K7QW4R5N8T6V2B3C6G", ParentID: "e_01J8X9M2K7QW4R5N8T6V2B3C6F", Created: ts,
			CallID: "call_1", Step: thread.StepDispatch, RunID: "s_1-t1"}, `"type":"approval_audit","v":6,`},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.e)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(b, []byte(c.v)) {
			t.Errorf("%s wire = %s, want %s", c.file, b, c.v)
		}
		wefttest.Golden(t, filepath.Join("testdata", "format6", c.file), append(b, '\n'))
		back, err := thread.UnmarshalEntry(b)
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		again, _ := json.Marshal(back)
		if !bytes.Equal(b, again) {
			t.Errorf("%s re-marshal differs\n got %s\nwant %s", c.file, again, b)
		}
		if !bytes.Contains(b, []byte(`"v":6`)) {
			continue
		}
		newer := bytes.Replace(b, []byte(`"v":6`), []byte(`"v":7`), 1)
		if _, err := thread.UnmarshalEntry(newer); !errors.Is(err, thread.ErrNewerFormat) {
			t.Errorf("%s at v=7: %v, want ErrNewerFormat", c.file, err)
		}
	}
	// A plain turn and a plain request keep the versions older readers read.
	plain, _ := json.Marshal(thread.TurnEntry{ID: "e_x", RunID: "s_1-t2"})
	if bytes.Contains(plain, []byte(`"v"`)) {
		t.Errorf("a plain turn entry carries a version: %s", plain)
	}
	msg, _ := json.Marshal(thread.MessageEntry{ID: "e_z", Message: core.User("hi")})
	if bytes.Contains(msg, []byte(`"v"`)) {
		t.Errorf("a plain message entry carries a version: %s", msg)
	}
	audit, _ := json.Marshal(thread.ApprovalAuditEntry{ID: "e_w", Step: thread.StepPark})
	if !bytes.Contains(audit, []byte(`"v":2,`)) {
		t.Errorf("a plain audit entry = %s, want v 2", audit)
	}
	req, _ := json.Marshal(thread.ApprovalRequestEntry{ID: "e_y", CallID: "c", Tool: "t", RunID: "r"})
	if !bytes.Contains(req, []byte(`"v":2,`)) {
		t.Errorf("a plain request = %s, want v 2", req)
	}
}
