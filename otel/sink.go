package otel

import (
	"context"

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

type sinkSpanExporter struct {
	exp   sdktrace.SpanExporter
	drops *dropCounter
}

func (e sinkSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	if err := e.exp.ExportSpans(ctx, spans); err != nil {
		e.drops.dropped(int64(len(spans)), err)
	}
	return nil
}

func (sinkSpanExporter) Shutdown(context.Context) error { return nil }

type sinkLogExporter struct {
	exp   sdklog.Exporter
	drops *dropCounter
}

func (e sinkLogExporter) Export(ctx context.Context, recs []sdklog.Record) error {
	if len(recs) == 0 {
		return nil
	}
	if err := e.exp.Export(ctx, recs); err != nil {
		e.drops.dropped(int64(len(recs)), err)
	}
	return nil
}

func (sinkLogExporter) Shutdown(context.Context) error   { return nil }
func (sinkLogExporter) ForceFlush(context.Context) error { return nil }
