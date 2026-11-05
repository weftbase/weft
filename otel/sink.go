package otel

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// A Sink's exporters run behind simple (synchronous) processors. A
// failed export is counted and named by the destination's throttled
// WARN, and the wrapper returns nil: handed back to the simple
// processor, the error would go to OTel's global error handler once per
// record — a stderr line per delta for as long as, say, the disk is
// full. Shutdown and ForceFlush stop here: the Sink's exporters are
// shut down by the pipeline itself, after the providers
// (destRuntime.closeSink), so its store outlives every flush.
//
// Every export is bounded (sinkGate): a synchronous export rides the
// run's goroutine, so a Sink that hangs — a store behind another
// process's lock, a stuck disk — would stall the run itself. Telemetry
// never blocks control: past the destination's Timeout (default
// defaultSinkTimeout) the export is abandoned and its records counted
// dropped, and while an abandoned export is still running every later
// one is dropped at once (at most one goroutine per signal is left
// behind) until it returns.

// defaultSinkTimeout bounds one Sink export when the destination sets
// no Timeout.
const defaultSinkTimeout = 2 * time.Second

// sinkGate bounds one signal's Sink exports.
type sinkGate struct {
	timeout time.Duration
	drops   *dropCounter
	// stalled: an abandoned export has not returned yet.
	stalled atomic.Bool
}

func newSinkGate(timeout time.Duration, drops *dropCounter) *sinkGate {
	if timeout <= 0 {
		timeout = defaultSinkTimeout
	}
	return &sinkGate{timeout: timeout, drops: drops}
}

// export runs fn under the gate's deadline, counting n records dropped
// when it fails, times out, or an earlier export is still stuck. fn
// must own what it exports: it may outlive this call.
func (g *sinkGate) export(ctx context.Context, n int64, fn func(context.Context) error) {
	if g.stalled.Load() {
		g.drops.dropped(n, errSinkStalled)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	select {
	case err := <-done:
		cancel()
		if err != nil {
			g.drops.dropped(n, err)
		}
	case <-ctx.Done():
		g.stalled.Store(true)
		g.drops.dropped(n, fmt.Errorf("sink export abandoned after %s: %w", g.timeout, context.Cause(ctx)))
		go func() {
			<-done
			cancel()
			g.stalled.Store(false)
		}()
	}
}

// errSinkStalled names the records a Sink refused while an abandoned
// export was still running.
var errSinkStalled = fmt.Errorf("sink export still stuck past its deadline; record dropped")

type sinkSpanExporter struct {
	exp  sdktrace.SpanExporter
	gate *sinkGate
}

func (e sinkSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	// The processor may reuse its slice once this returns; the spans
	// themselves are immutable snapshots.
	own := append([]sdktrace.ReadOnlySpan(nil), spans...)
	e.gate.export(ctx, int64(len(own)), func(ctx context.Context) error { return e.exp.ExportSpans(ctx, own) })
	return nil
}

func (sinkSpanExporter) Shutdown(context.Context) error { return nil }

type sinkLogExporter struct {
	exp  sdklog.Exporter
	gate *sinkGate
}

func (e sinkLogExporter) Export(ctx context.Context, recs []sdklog.Record) error {
	if len(recs) == 0 {
		return nil
	}
	// The simple processor pools its slice, and the processors after
	// this one may change the record: the export may outlive this call,
	// so it gets copies of its own.
	own := make([]sdklog.Record, len(recs))
	for i := range recs {
		own[i] = recs[i].Clone()
	}
	e.gate.export(ctx, int64(len(own)), func(ctx context.Context) error { return e.exp.Export(ctx, own) })
	return nil
}

func (sinkLogExporter) Shutdown(context.Context) error   { return nil }
func (sinkLogExporter) ForceFlush(context.Context) error { return nil }
