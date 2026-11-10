package thread_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// Thread records held for a run that has not reported are bounded
// (256); past the bound the oldest go. The loss is visible (review
// finding C4): a WARN names it when it first happens, and Close's line
// is a WARN with the count — never a Debug line a production logger
// drops.
func TestHeldRecordOverflowIsWarned(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script(wefttest.Say("hi")), core.Logger(logger)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Custom(bg, "app/note", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	// No run has reported: every label's record is held.
	for i := range 300 {
		if err := s.Label(bg, s.Leaf(), fmt.Sprintf("l%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(bg); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "records dropped") {
		t.Fatalf("log = %q, want a WARN naming the dropped records", out)
	}
}
