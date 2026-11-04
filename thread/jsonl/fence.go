package jsonl

import (
	"context"
	"fmt"
	"os"

	"github.com/weftbase/weft/thread"
)

// Fence is the thread.Fencer capability: nil while this instance holds
// the session's file and the file at the session's path is still that
// file — the lock (flock) is held for as long as the file is open, so
// what can change under a writer is the file itself: deleted, or
// replaced by another writer's. ErrLocked when this instance does not
// hold the session (released, or never written through it); ErrStale
// when the path names another file or none.
func (b *backend) Fence(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	s, ok := b.sessions[id]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %s is not held by this writer", thread.ErrLocked, id)
	}
	<-s.ready
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.f == nil {
		return fmt.Errorf("%w: %s is not held by this writer", thread.ErrLocked, id)
	}
	held, err := s.f.Stat()
	if err != nil {
		return err
	}
	cur, err := os.Stat(b.path(id))
	if err != nil || !os.SameFile(held, cur) {
		return fmt.Errorf("%w: %s: the session's file was removed or replaced under this writer", thread.ErrStale, id)
	}
	return nil
}
