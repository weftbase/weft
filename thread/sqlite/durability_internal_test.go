package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/weftbase/weft/thread"
)

// Every commit fsyncs the write-ahead log, under either fsync policy:
// synchronous=FULL in WAL mode, set in the connection's DSN (review
// 3 P2-4). FsyncOnFlush used to be NORMAL, leaving the sync to Flush's
// passive checkpoint, which a reader holding an old snapshot leaves with
// nothing to do — and which syncs nothing then: the dispatch record the
// gate flushed before a handler started could be lost to a power cut.
// Before that, the backend ran NORMAL whatever was asked.
func TestEveryCommitIsSynced(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []thread.OpenOption
	}{
		{"default", nil},
		{"FsyncEveryAppend", []thread.OpenOption{thread.FsyncEveryAppend()}},
		{"FsyncOnFlush", []thread.OpenOption{thread.FsyncOnFlush()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := Open(filepath.Join(t.TempDir(), "sessions.db"), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := st.Create(ctx, thread.Header{ID: "s_sync", Created: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			// The working connection (a file database has one; a
			// replacement is opened from the same DSN, pragma and all).
			var level int
			var mode string
			db := st.(*backend).db
			if err := db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&level); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
				t.Fatal(err)
			}
			if level != 2 || mode != "wal" {
				t.Errorf("synchronous=%d journal_mode=%s, want 2 (FULL) and wal", level, mode)
			}
			// Flush has nothing left to sync; it still answers for the
			// session.
			if err := st.(thread.Flusher).Flush(ctx, "s_sync"); err != nil {
				t.Errorf("Flush = %v", err)
			}
			if err := st.(thread.Flusher).Flush(ctx, "s_none"); !errors.Is(err, thread.ErrNotFound) {
				t.Errorf("Flush of a missing session = %v, want ErrNotFound", err)
			}
		})
	}
}
