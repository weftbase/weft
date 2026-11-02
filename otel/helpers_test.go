package otel

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/wefttest"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func testCtx(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

func enabledParams(name string) log.EnabledParameters {
	return log.EnabledParameters{EventName: name}
}

func base64Std(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// memExporter is an in-memory log exporter (the "Exporters(inMemory)"
// of the S7 gates).
type memExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func newMemExporter() *memExporter { return &memExporter{} }

func (m *memExporter) Export(_ context.Context, recs []sdklog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range recs {
		m.records = append(m.records, r.Clone())
	}
	return nil
}

func (m *memExporter) Shutdown(context.Context) error   { return nil }
func (m *memExporter) ForceFlush(context.Context) error { return nil }

func (m *memExporter) snapshot() []sdklog.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sdklog.Record, len(m.records))
	copy(out, m.records)
	return out
}

// agentThrough runs one scripted agent run against the pipeline's
// providers (hermetic: NoGlobal, providers passed explicitly). The run
// is two steps (a tool step and a final say), so its durable events
// are run_start, step_start, tool_start, tool_finish, step_finish,
// step_start, step_finish, run_finish — eight.
func agentThrough(t *testing.T, p *Pipeline) (*core.RunResult, string) {
	t.Helper()
	slow := core.Tool("slow", "Sleeps briefly.", func(ctx context.Context, in struct {
		Note string `json:"note"`
	}) (string, error) {
		time.Sleep(30 * time.Millisecond)
		return "done:" + in.Note, nil
	})
	agt := core.New(
		wefttest.Script(
			wefttest.Think("plan", wefttest.ToolCalls(wefttest.Call{Name: "slow", Args: `{"note":"one"}`})),
			wefttest.Say("all done"),
		),
		core.Name("s7"),
		core.TracerProvider(p.TracerProvider()),
		core.LoggerProvider(p.LoggerProvider()),
		slow,
	)
	res, err := agt.Generate(context.Background(), core.Prompt("go"),
		core.Metadata(map[string]string{"weft.session.id": "s7-session", "tenant": "acme"}))
	if err != nil {
		t.Fatal(err)
	}
	return res, res.ID
}

// sdkRecordWith builds one SDK record through a real provider — a bare
// sdklog.Record keeps no attributes (the provider sets the limits the
// record's store consults).
func sdkRecordWith(t *testing.T, eventName, body string, attrs ...attribute.KeyValue) *sdklog.Record {
	t.Helper()
	capture := &recordCapture{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(capture)),
	)
	defer func() { _ = provider.Shutdown(context.Background()) }()
	var r log.Record
	r.SetEventName(eventName)
	if body != "" {
		r.SetBody(attribute.StringValue(body))
	}
	r.AddAttributes(attrs...)
	provider.Logger("github.com/weftgo/weft/core").Emit(context.Background(), r)
	if len(capture.records) == 0 {
		t.Fatal("no record captured")
	}
	return &capture.records[0]
}

// attrOf reads one attribute off a record.
func attrOf(r sdklog.Record, key string) string {
	var out string
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			out = kv.Value.AsString()
			return false
		}
		return true
	})
	return out
}

// recordCapture is a log exporter that keeps what it was handed.
type recordCapture struct {
	records []sdklog.Record
}

func (c *recordCapture) Export(_ context.Context, recs []sdklog.Record) error {
	for _, r := range recs {
		c.records = append(c.records, r.Clone())
	}
	return nil
}

func (c *recordCapture) Shutdown(context.Context) error   { return nil }
func (c *recordCapture) ForceFlush(context.Context) error { return nil }

// stubSpan builds a snapshot span; statusCode is the SDK's code
// (codes.Ok=2, codes.Error=1).
func stubSpan(res *sdkresource.Resource, traceID trace.TraceID, spanID, parent trace.SpanID, name string, kind int, start, end time.Time, statusCode int, statusMsg string, attrs []attribute.KeyValue, events []sdktrace.Event) sdktrace.ReadOnlySpan {
	return tracetest.SpanStub{
		Name:        name,
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}),
		Parent:      trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: parent}),
		SpanKind:    trace.SpanKind(kind),
		StartTime:   start,
		EndTime:     end,
		Attributes:  attrs,
		Events:      events,
		Status:      sdktrace.Status{Code: otelcodes.Code(statusCode), Description: statusMsg},
		Resource:    res,
	}.Snapshot()
}

func mustTraceID(t *testing.T, s string) trace.TraceID {
	t.Helper()
	id, err := trace.TraceIDFromHex(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustSpanID(t *testing.T, s string) trace.SpanID {
	t.Helper()
	id, err := trace.SpanIDFromHex(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// memSink is an in-memory Sink — what a store behind otel.Sink receives,
// synchronously: the framework-side stand-in for a local viewer's store.
type memSink struct {
	spans *tracetest.InMemoryExporter
	logs  *memExporter

	mu       sync.Mutex
	fail     error         // when set, every export fails with it
	block    chan struct{} // when set, every export waits for it to close
	opened   int
	shutdown int
}

func newMemSink() *memSink {
	return &memSink{spans: tracetest.NewInMemoryExporter(), logs: newMemExporter()}
}

// option is the Sink destination over m.
func (m *memSink) option(opts ...DestOption) Option { return Sink("mem", m.open, opts...) }

func (m *memSink) open(context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error) {
	m.mu.Lock()
	m.opened++
	m.mu.Unlock()
	return memSinkSpans{m}, memSinkLogs{m}, nil
}

func (m *memSink) failing() error {
	m.mu.Lock()
	block := m.block
	m.mu.Unlock()
	if block != nil {
		<-block
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fail
}

func (m *memSink) setBlock(ch chan struct{}) {
	m.mu.Lock()
	m.block = ch
	m.mu.Unlock()
}

func (m *memSink) setFail(err error) {
	m.mu.Lock()
	m.fail = err
	m.mu.Unlock()
}

func (m *memSink) shutdowns() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.shutdown
}

// records is every log record the sink was handed.
func (m *memSink) records() []sdklog.Record { return m.logs.snapshot() }

// spanStubs is every span the sink was handed.
func (m *memSink) spanStubs() tracetest.SpanStubs { return m.spans.GetSpans() }

// eventTypes lists the weft.event records' types, in order.
func (m *memSink) eventTypes() []string {
	var out []string
	for _, r := range m.records() {
		if attrOf(r, "weft.record") == "event" {
			out = append(out, attrOf(r, "weft.event.type"))
		}
	}
	return out
}

// ofRecord returns the records whose weft.record attribute is kind.
func (m *memSink) ofRecord(kind string) []sdklog.Record {
	var out []sdklog.Record
	for _, r := range m.records() {
		if attrOf(r, "weft.record") == kind {
			out = append(out, r)
		}
	}
	return out
}

type memSinkSpans struct{ m *memSink }

func (e memSinkSpans) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.m.failing(); err != nil {
		return err
	}
	return e.m.spans.ExportSpans(ctx, spans)
}

func (e memSinkSpans) Shutdown(context.Context) error {
	e.m.mu.Lock()
	e.m.shutdown++
	e.m.mu.Unlock()
	return nil
}

type memSinkLogs struct{ m *memSink }

func (e memSinkLogs) Export(ctx context.Context, recs []sdklog.Record) error {
	if err := e.m.failing(); err != nil {
		return err
	}
	return e.m.logs.Export(ctx, recs)
}

func (e memSinkLogs) Shutdown(context.Context) error {
	e.m.mu.Lock()
	e.m.shutdown++
	e.m.mu.Unlock()
	return nil
}

func (memSinkLogs) ForceFlush(context.Context) error { return nil }
