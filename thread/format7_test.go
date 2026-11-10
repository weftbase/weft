package thread_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// Format 7 (ADR 0032, the 2026-11-10 amendment): a key record — the
// call runs under an inherited key — is written with "v":7, so a
// reader from before inheritance (format 6) fails loudly instead of
// retrying the call under its occurrence key. Golden-gated.
func TestKeyRecordFormatGolden(t *testing.T) {
	e := thread.ApprovalAuditEntry{
		ID: "e_k", ParentID: "e_p", Created: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC),
		CallID: "s2", Step: thread.StepKey, RunID: "s_1-t2",
		Entry: "e_a", Tool: "ship", ArgsSHA256: "ab", Key: "0123456789abcdef0123456789abcdef",
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"v":7`)) {
		t.Fatalf("wire = %s, want v 7", b)
	}
	wefttest.Golden(t, filepath.Join("testdata", "format7", "approval_audit_key.json"), append(b, '\n'))
	back, err := thread.UnmarshalEntry(b)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := json.Marshal(back); !bytes.Equal(again, b) {
		t.Fatalf("re-marshal differs\n got %s\nwant %s", again, b)
	}
	if _, err := thread.UnmarshalEntry(bytes.Replace(b, []byte(`"v":7`), []byte(`"v":8`), 1)); !errors.Is(err, thread.ErrNewerFormat) {
		t.Fatalf("at v=8: %v, want ErrNewerFormat", err)
	}
	// The same entry at v6 — what a format-6 build reads up to — is not
	// what this build writes: a format-6 reader refuses the v7 line.
	if bytes.Contains(b, []byte(`"v":6`)) {
		t.Fatal("unreachable")
	}
}
