package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/weftbase/weft/thread"
)

// Fence is the thread.Fencer capability: one read of the session's
// lock row. Nil while this instance holds the row; ErrLocked when it
// never took it here or another writer has it now (BreakLock, a
// takeover) — the instance then forgets its hold, as its next Append
// would.
func (b *backend) Fence(ctx context.Context, id string) (err error) {
	defer func() { err = txCanceled(ctx, err) }()
	b.mu.Lock()
	held := b.held[id] != nil
	b.mu.Unlock()
	if !held {
		return fmt.Errorf("%w: %s is not held by this writer", thread.ErrLocked, id)
	}
	var owner string
	err = b.db.QueryRowContext(ctx, `SELECT owner FROM session_locks WHERE session = ?`, id).Scan(&owner)
	switch {
	case err == nil && owner == b.owner:
		return nil
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return err
	}
	b.mu.Lock()
	delete(b.held, id)
	b.mu.Unlock()
	return fmt.Errorf("%w: %s (this writer's lock was broken; the session may have been written since — open it again)", thread.ErrLocked, id)
}
