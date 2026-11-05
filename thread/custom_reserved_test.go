package thread_test

import (
	"errors"
	"testing"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/thread"
	"github.com/weftbase/weft/wefttest"
)

// Custom refuses a kind under the format's reserved "weft/" prefix
// (review WR-23): "weft/pin" is what Pin writes and compaction reads,
// so application state could otherwise forge a pin. Nothing is
// written; an application's own kinds are untouched.
func TestCustomRefusesAReservedKind(t *testing.T) {
	s, err := thread.Create(bg, thread.Memory(), core.New(wefttest.Script()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(bg) }()
	before := len(s.Entries())
	for _, kind := range []string{"weft/pin", "weft/anything"} {
		if err := s.Custom(bg, kind, []byte(`"e_x"`)); !errors.Is(err, thread.ErrReservedKey) {
			t.Fatalf("Custom(%q) = %v, want ErrReservedKey", kind, err)
		}
	}
	if n := len(s.Entries()); n != before {
		t.Fatalf("a refused Custom wrote %d entries", n-before)
	}
	for _, kind := range []string{"weft", "weft.pin", "app/weft/pin", "cart"} {
		if err := s.Custom(bg, kind, nil); err != nil {
			t.Errorf("Custom(%q) = %v, want accepted", kind, err)
		}
	}
}
