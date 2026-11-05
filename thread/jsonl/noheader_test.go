package jsonl_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
)

// A file with no complete line — empty, or a torn first line, what a
// crash inside Create leaves — is ErrCorrupt on line 1 wrapping
// io.ErrUnexpectedEOF; a complete first line that does not decode is
// line-1 corruption too, but never that: thread/pool reads only the
// first as a child that was never created.
func TestNoHeaderIsToldApartFromABadHeader(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		bytes   string
		creates bool // reads as a creation that never completed
	}{
		{"empty", "", true},
		{"torn first line", `{"weft":7,"id":"s_x`, true},
		{"complete bad line", "{not json}\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			const id = "s_noheader"
			if err := os.WriteFile(filepath.Join(dir, id+".jsonl"), []byte(tc.bytes), 0o600); err != nil {
				t.Fatal(err)
			}
			st, err := jsonl.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, _, _, err = st.Load(ctx, id)
			var ce *thread.CorruptError
			if !errors.As(err, &ce) || ce.Line != 1 {
				t.Fatalf("Load = %v, want a CorruptError on line 1", err)
			}
			if got := errors.Is(err, io.ErrUnexpectedEOF); got != tc.creates {
				t.Fatalf("errors.Is(err, io.ErrUnexpectedEOF) = %v, want %v (err %v)", got, tc.creates, err)
			}
		})
	}
}
