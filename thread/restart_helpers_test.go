package thread_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/thread/jsonl"
	"github.com/weftbase/weft/thread/sqlite"
)

// durableBackend is one file-backed store a restart test reopens: open
// returns a fresh Storage value over the same path each call — what a
// new process gets.
type durableBackend struct {
	name string
	open func(t *testing.T) thread.Storage
}

// durableBackends are the two file-backed stores, each over its own
// temp path for the test.
func durableBackends(t *testing.T) []durableBackend {
	t.Helper()
	jdir := t.TempDir()
	sdb := filepath.Join(t.TempDir(), "s.db")
	return []durableBackend{
		{name: "jsonl", open: func(t *testing.T) thread.Storage {
			st, err := jsonl.Open(jdir)
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
		{name: "sqlite", open: func(t *testing.T) thread.Storage {
			st, err := sqlite.Open(sdb)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeStore(st) })
			return st
		}},
	}
}

// restart stands in a process restart for session id: the old store's
// hold on it is released, the store is closed, and a fresh Storage
// value over the same path is returned.
func restart(t *testing.T, b durableBackend, st thread.Storage, id string) thread.Storage {
	t.Helper()
	abandon(t, st, id)
	closeStore(st)
	return b.open(t)
}

func closeStore(st thread.Storage) {
	if c, ok := st.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

var bg = context.Background()
