// Package ladder holds the recovery ladder's one timing a test outside
// package thread needs to shorten (ADR 0032): how long the end of a
// turn waits for a handler the loop abandoned before it parks the call
// for a person as still running. thread reads it; thread's and
// thread/pool's tests set it in TestMain, before any session runs, and
// never while one does.
package ladder

import "time"

// DefaultAbandonedWait is the production bound (TestRecoveryTimingDefaults
// pins it).
const DefaultAbandonedWait = 30 * time.Second

// AbandonedWait is the bound in force. Set only from a TestMain.
var AbandonedWait = DefaultAbandonedWait
