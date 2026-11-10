package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/weftbase/weft"
	"github.com/weftbase/weft/thread"
)

// erasureMarker is the personal data the erasure tests write and then
// look for, byte for byte, in every file the database keeps.
const erasureMarker = "SSN-123-45-6789-ALICE-PRIVATE"

// markerCount counts the marker in the database file and its -wal.
func markerCount(t *testing.T, path string) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, f := range []string{path, path + "-wal"} {
		b, err := os.ReadFile(f)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		out[filepath.Base(f)] = bytes.Count(b, []byte(erasureMarker))
	}
	return out
}

func noMarker(t *testing.T, when string, counts map[string]int) {
	t.Helper()
	for f, n := range counts {
		if n != 0 {
			t.Errorf("%s: %s still holds the deleted session's text %d times", when, f, n)
		}
	}
}

// seedErasure creates s_pii with twenty marker messages beside
// s_other, which keeps pages of the file in use.
func seedErasure(t *testing.T, st thread.Storage) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"s_pii", "s_other"} {
		if err := st.Create(ctx, thread.Header{ID: id, Created: now}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 {
		e := thread.MessageEntry{ID: fmt.Sprintf("e_%02d", i), Created: now, Message: weft.User(erasureMarker + " order details")}
		if err := st.Append(ctx, "s_pii", e); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Append(ctx, "s_other", thread.MessageEntry{ID: "e_keep", Created: now, Message: weft.User("kept")}); err != nil {
		t.Fatal(err)
	}
}

// A Delete that returned nil leaves none of the session's bytes
// readable in the database file or its write-ahead log — not while
// the Storage stays open, not after Close (C1: freed pages kept their
// cells and the WAL kept every frame that ever held them, so the
// erased conversation was readable on disk with a hex dump).
func TestDeleteErasesSessionBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	seedErasure(t, st)
	ctx := context.Background()
	if before := markerCount(t, path); before["sessions.db"]+before["sessions.db-wal"] == 0 {
		t.Fatalf("the marker never reached disk (%v): the test proves nothing", before)
	}
	if err := thread.Delete(ctx, st, "s_pii"); err != nil {
		t.Fatal(err)
	}
	noMarker(t, "open, after Delete", markerCount(t, path))
	// The other session is untouched.
	if _, entries, _, err := st.Load(ctx, "s_other"); err != nil || len(entries) != 1 {
		t.Fatalf("s_other: %d entries, %v", len(entries), err)
	}
	if err := st.(*backend).Close(); err != nil {
		t.Fatal(err)
	}
	noMarker(t, "after Close", markerCount(t, path))
}

// A reader that keeps the write-ahead log in use (another process's
// open read transaction) blocks the truncating checkpoint. Delete then
// does not report success: the rows are gone, but their bytes are
// still in the log, so it returns ErrErasureIncomplete — and
// Checkpoint, once the reader is gone, finishes the erasure.
func TestDeleteReportsErasureBlockedByReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.(*backend).Close() }()
	seedErasure(t, st)
	b := st.(*backend)
	b.eraseWait = 0 // give up on a blocked checkpoint at once

	reader, err := sql.Open("sqlite", "file:"+escapeURIPath(path)+"?"+connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	ctx := context.Background()
	tx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM entries`).Scan(&n); err != nil {
		t.Fatal(err)
	}

	err = thread.Delete(ctx, st, "s_pii")
	if !errors.Is(err, ErrErasureIncomplete) {
		t.Fatalf("Delete under a reader: %v, want ErrErasureIncomplete", err)
	}
	if _, _, _, err := st.Load(ctx, "s_pii"); !errors.Is(err, thread.ErrNotFound) {
		t.Fatalf("Load after the incomplete erasure: %v, want ErrNotFound", err)
	}
	if err := Checkpoint(ctx, st); !errors.Is(err, ErrErasureIncomplete) {
		t.Fatalf("Checkpoint under the reader: %v, want ErrErasureIncomplete", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := Checkpoint(ctx, st); err != nil {
		t.Fatalf("Checkpoint after the reader left: %v", err)
	}
	noMarker(t, "open, after Checkpoint", markerCount(t, path))
}
