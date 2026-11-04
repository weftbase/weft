package thread

import (
	"os"
	"testing"
	"time"

	"github.com/weftbase/weft/thread/internal/ladder"
)

// TestMain shortens the recovery ladder's waits for the package's
// tests: the keyed-retry backoff (1 s doubling in production) and the
// bound on waiting for an abandoned handler (30 s). The production
// values are pinned by TestRecoveryTimingDefaults.
func TestMain(m *testing.M) {
	retryBackoffBase = 10 * time.Millisecond
	ladder.AbandonedWait = 2 * time.Second
	os.Exit(m.Run())
}

// The production timings of the recovery ladder (ADR 0032): a keyed
// retry waits 1 s, 2 s, 4 s (doubling, capped at 30 s), and the end of
// a turn waits at most 30 s for an abandoned handler to return.
func TestRecoveryTimingDefaults(t *testing.T) {
	if defaultRetryBackoffBase != time.Second || retryBackoffCap != 30*time.Second || ladder.DefaultAbandonedWait != 30*time.Second {
		t.Fatalf("backoff %v cap %v wait %v", defaultRetryBackoffBase, retryBackoffCap, ladder.DefaultAbandonedWait)
	}
}
