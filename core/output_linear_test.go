package core

import (
	"strings"
	"testing"
)

type linearOut struct {
	Items []string `json:"items"`
}

// feedItems streams a submission of n array elements, one per delta,
// and returns the decoder, the partials it reported and the total bytes
// fed.
func feedItems(n int) (d *OutputDecoder[linearOut], partials []linearOut, total int) {
	d = NewOutputDecoder[linearOut]()
	d.Feed(StepStart{})
	feed := func(s string) {
		total += len(s)
		if p, ok := d.Feed(ToolArgsDelta{Name: outputToolName, Args: s}); ok {
			partials = append(partials, p)
		}
	}
	feed(`{"items":[`)
	for i := range n {
		sep := ","
		if i == 0 {
			sep = ""
		}
		feed(sep + `"abcdefgh"`)
	}
	feed(`]}`)
	return d, partials, total
}

// S3: OutputDecoder.Feed's total work is linear in the submission's
// length, not quadratic: the bytes every decode attempt read, summed
// over a streamed submission, stay within a constant factor of what
// was fed (plus the per-delta window kept fresh for small
// submissions). Deterministic: it counts bytes scanned, not time.
func TestOutputDecoderWorkIsLinear(t *testing.T) {
	const factor = 16
	for _, n := range []int{2000, 8000, 32000} {
		d, partials, total := feedItems(n)
		bound := factor*total + outputDecodeEager*outputDecodeEager/10
		if d.scanned > bound {
			t.Errorf("n=%d: decode attempts read %d bytes for a %d-byte submission (bound %d): quadratic", n, d.scanned, total, bound)
		}
		if len(partials) == 0 {
			t.Fatalf("n=%d: no partial reported", n)
		}
		// Partials only grow, and the stream's end is reached: the last
		// delta closes the document, whose decode is due.
		for i := 1; i < len(partials); i++ {
			if len(partials[i].Items) < len(partials[i-1].Items) {
				t.Fatalf("n=%d: partial %d shrank: %d < %d items", n, i, len(partials[i].Items), len(partials[i-1].Items))
			}
		}
	}
}

// The whole-call ToolStart that follows the deltas always decodes: the
// partial a UI shows last is the complete submission, whatever deltas
// the spacing skipped, and Result is exact.
func TestOutputDecoderToolStartDecodesWhole(t *testing.T) {
	d, _, _ := feedItems(20000)
	args := d.buf.String()
	p, _ := d.Feed(ToolStart{Name: outputToolName, Args: []byte(args)})
	if len(p.Items) != 20000 {
		t.Fatalf("partial after ToolStart has %d items, want 20000", len(p.Items))
	}
	d.Feed(ToolFinish{Name: outputToolName})
	out, err := d.Result()
	if err != nil || len(out.Items) != 20000 || out.Items[19999] != "abcdefgh" {
		t.Fatalf("Result = %d items, %v", len(out.Items), err)
	}
	if !strings.HasSuffix(args, `"]}`) {
		t.Fatalf("fed arguments end %q", args[len(args)-8:])
	}
}

// Small submissions keep a fresh partial on every delta that changes
// it — the spacing starts only past outputDecodeEager bytes.
func TestOutputDecoderSmallSubmissionsDecodeEveryDelta(t *testing.T) {
	_, partials, total := feedItems(50)
	if total > outputDecodeEager {
		t.Fatalf("test submission is %d bytes, not small", total)
	}
	// One partial per delta, as before the spacing: the opening delta's
	// (an empty array) and one per element delta after it.
	if len(partials) != 51 {
		t.Errorf("%d partials for 52 deltas of 50 elements, want 51 (every delta that changed it)", len(partials))
	}
}
