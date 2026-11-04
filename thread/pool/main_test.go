package pool_test

import (
	"os"
	"testing"
	"time"

	"github.com/weftbase/weft/thread/internal/ladder"
)

// TestMain shortens the bound the end of a turn waits for an abandoned
// handler (30 s in production) to thread's own tests' 2 s, so a child's
// still-running handler parks its call within a test's time.
func TestMain(m *testing.M) {
	ladder.AbandonedWait = 2 * time.Second
	os.Exit(m.Run())
}
