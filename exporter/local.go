package exporter

import (
	"context"
	"sync"

	"github.com/weftgo/weft/obsdb"
	"github.com/weftgo/weft/obsdb/sqlite"
	"github.com/weftgo/weft/otel"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Local is the local sink: obsdb/sqlite at path ("" = $WEFT_DB or
// ./.weft/weft.db), opened when the pipeline starts and written
// synchronously through otel.Sink — nothing emitted is lost, and the
// DB's in-process hub is the live lane a Studio sharing the handle
// (studio.DB(exporter.LocalDB())) reads. Content on, traces + logs, raw
// bodies and app logs eligible (a Sink is first-party). Delta records
// reach the store and are counted by Write, not stored. The database
// closes when the pipeline shuts down. Naming a Local switches the
// discovery file off in Install and Start: the pipeline writes to the
// stores it names (and the environment's), never into a running Studio
// it found on disk — add Studio(url, token) to write to one as well.
func Local(path string, opts ...otel.DestOption) otel.Option {
	m := &marker{kind: kindLocal}
	m.opt = otel.Options(otel.Sink("local", func(context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error) {
		p := path
		if p == "" {
			p = defaultLocalPath(envGetenv)
		}
		db, err := sqlite.Open(p)
		if err != nil {
			return nil, nil, err
		}
		h := openHandle(db, m)
		return &localSpanExporter{h: h}, &localLogExporter{h: h}, nil
	}, opts...))
	register(m)
	return m.opt
}

// defaultLocalPath is where Local writes when no path is given: $WEFT_DB
// or ./.weft/weft.db.
func defaultLocalPath(getenv func(string) string) string {
	if p := getenv("WEFT_DB"); p != "" {
		return p
	}
	return ".weft/weft.db"
}

// localHandle is one opened Local database, shared by its span and log
// exporters: it closes when both have been shut down (otel shuts a
// Sink's exporters down after the providers, or at once when the
// pipeline fails to build).
type localHandle struct {
	db     obsdb.DB
	m      *marker
	mu     sync.Mutex
	refs   int
	closed bool
}

func openHandle(db obsdb.DB, m *marker) *localHandle {
	h := &localHandle{db: db, m: m, refs: 2}
	regMu.Lock()
	openLocals = append(openLocals, h)
	m.handle = h
	regMu.Unlock()
	return h
}

func (h *localHandle) release() error {
	h.mu.Lock()
	h.refs--
	last := h.refs == 0 && !h.closed
	if last {
		h.closed = true
	}
	h.mu.Unlock()
	if !last {
		return nil
	}
	regMu.Lock()
	for i, o := range openLocals {
		if o == h {
			openLocals = append(openLocals[:i], openLocals[i+1:]...)
			break
		}
	}
	if h.m.handle == h {
		h.m.handle = nil
	}
	regMu.Unlock()
	return h.db.Close()
}

func (h *localHandle) isOpen() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.closed
}

// localSpanExporter writes SDK spans into the shared obsdb.DB. A failed
// write returns its error: otel's Sink counts it as the destination's
// drop and names it in its throttled WARN.
type localSpanExporter struct {
	h    *localHandle
	once sync.Once
}

func (e *localSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	return e.h.db.Write(ctx, obsdb.Batch{Spans: FromSDKSpans(spans)})
}

func (e *localSpanExporter) Shutdown(context.Context) error {
	var err error
	e.once.Do(func() { err = e.h.release() })
	return err
}

// localLogExporter writes SDK log records into the shared obsdb.DB.
type localLogExporter struct {
	h    *localHandle
	once sync.Once
}

func (e *localLogExporter) Export(ctx context.Context, recs []sdklog.Record) error {
	if len(recs) == 0 {
		return nil
	}
	return e.h.db.Write(ctx, obsdb.Batch{Records: FromSDKRecords(recs)})
}

func (e *localLogExporter) Shutdown(context.Context) error {
	var err error
	e.once.Do(func() { err = e.h.release() })
	return err
}

func (e *localLogExporter) ForceFlush(context.Context) error { return nil }
