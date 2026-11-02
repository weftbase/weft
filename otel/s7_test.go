package otel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/weftgo/weft/core"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	// The kill-9 and heartbeat-row tests read the SQLite file directly.
	_ "modernc.org/sqlite"
)

// S7 gate 1: a Sink + Exporters(inMemory) + a stripped destination at
// once — the Sink (first-party) has content, the stripped one has none,
// both have every durable record (the clone rule: no chain sees
// another's edits). The store-side form of the gate (the transcript
// rebuilt byte-for-byte from obsdb) is weft/exporter's.
func TestS7SinkPlusMemoryPlusStripped(t *testing.T) {
	sink := newMemSink()
	stripped := newMemExporter()
	p, err := Start(testCtx(t), NoGlobal(), NoEnv(),
		sink.option(),
		Exporters(nil, stripped, NoContent()),
	)
	if err != nil {
		t.Fatal(err)
	}
	res, runID := agentThrough(t, p)
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}

	// The Sink has content: the messages records concatenate to the
	// run's transcript byte-for-byte.
	msgs := sink.ofRecord("messages")
	if len(msgs) == 0 {
		t.Fatal("the Sink received no messages records — its content default is on")
	}
	rebuilt := []byte{'['}
	for i, r := range msgs {
		if attrOf(r, "weft.run.id") != runID {
			t.Errorf("messages record of run %q", attrOf(r, "weft.run.id"))
		}
		batch := r.Body().AsString()
		if i > 0 {
			rebuilt = append(rebuilt, ',')
		}
		rebuilt = append(rebuilt, batch[1:len(batch)-1]...) // drop the outer [ ]
	}
	rebuilt = append(rebuilt, ']')
	want, err := json.Marshal(res.Messages)
	if err != nil {
		t.Fatal(err)
	}
	if string(rebuilt) != string(want) {
		t.Fatalf("Sink transcript mismatch:\n got %s\nwant %s", rebuilt, want)
	}

	// The stripped destination: no content, no messages records, every
	// durable record.
	var events, messages int
	for _, r := range stripped.snapshot() {
		switch attrOf(r, "weft.record") {
		case "messages":
			messages++
		case "event":
			events++
			if attrOf(r, "weft.content") != "stripped" {
				t.Errorf("event record not marked stripped: %q", attrOf(r, "weft.content"))
			}
			if attrOf(r, "weft.event.type") == "tool_finish" {
				var body map[string]any
				_ = json.Unmarshal([]byte(r.Body().AsString()), &body)
				if body["content"] != "" {
					t.Error("stripped tool_finish kept its content")
				}
			}
		case "delta":
			if attrOf(r, "weft.content") != "stripped" {
				t.Error("delta record not marked stripped")
			}
		}
	}
	if messages != 0 {
		t.Errorf("stripped destination received %d messages records, want 0", messages)
	}
	if events != 8 {
		t.Errorf("stripped destination saw %d event records, want 8 (every durable record of a two-step run)", events)
	}

	// And the Sink saw every durable record too, with content.
	if got := sink.eventTypes(); len(got) != 8 {
		t.Errorf("the Sink saw %d event records, want 8: %v", len(got), got)
	}
	for _, r := range sink.ofRecord("event") {
		if attrOf(r, "weft.content") == "stripped" {
			t.Error("the Sink received a stripped event")
		}
	}
	if sink.shutdowns() != 2 {
		t.Errorf("the Sink's exporters were shut down %d times, want 2 (one each)", sink.shutdowns())
	}
}

// S7 gate 2: a pipeline whose only destinations are content-off makes
// the core emit no messages records (the Enabled rule).
func TestS7ContentOffOnlyEmitsNoMessages(t *testing.T) {
	mem := newMemExporter()
	p, err := Start(testCtx(t), NoGlobal(), NoEnv(),
		Exporters(nil, mem, NoContent()),
		OTLP("https://collector.invalid:4318"),
	)
	if err != nil {
		t.Fatal(err)
	}
	res, _ := agentThrough(t, p)
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	var messages int
	for _, r := range mem.snapshot() {
		if r.EventName() == "weft.messages" || attrOf(r, "weft.record") == "messages" {
			messages++
		}
	}
	if messages != 0 {
		t.Errorf("content-off-only pipeline: %d messages records reached a destination", messages)
	}
	if len(res.Messages) == 0 {
		t.Fatal("the run itself produced no transcript (test bug)")
	}
}

// The tracker: open runs tracked from run_start/run_finish records and
// the invoke_agent span end; heartbeats ignored (no recursion).
func TestRunTrackerOpenRuns(t *testing.T) {
	tr := newRunTracker()
	emit := func(kind, eventType, runID string) {
		attrs := []attribute.KeyValue{
			attribute.String("weft.record", kind),
			attribute.String("weft.run.id", runID),
		}
		if eventType != "" {
			attrs = append(attrs, attribute.String("weft.event.type", eventType))
		}
		_ = tr.OnEmit(context.Background(), sdkRecordWith(t, "weft."+kind, "", attrs...))
	}
	emit("event", "run_start", "r1")
	emit("event", "run_start", "r2")
	if got := tr.snapshot(); len(got) != 2 {
		t.Fatalf("open runs = %v", got)
	}
	emit("heartbeat", "", "r1") // ignored, not a new run
	if got := tr.snapshot(); len(got) != 2 {
		t.Fatalf("heartbeat changed the open set: %v", got)
	}
	emit("event", "run_finish", "r2")
	if got := tr.snapshot(); len(got) != 1 {
		t.Fatalf("after run_finish: %v", got)
	}
	// The failed-run path: no run_finish, but the invoke_agent span
	// ends.
	sp := &trackerSpanProc{t: tr}
	stub := stubSpan(nil, mustTraceID(t, "4bf92f3577b34da6a3ce929d0e0e4736"),
		mustSpanID(t, "00f067aa0ba902b7"), mustSpanID(t, "00f067aa0ba902b7"),
		"invoke_agent x", 1, time.Now(), time.Now(), 1, "", []attribute.KeyValue{
			attribute.String("gen_ai.operation.name", "invoke_agent"),
			attribute.String("weft.run.id", "r1"),
		}, nil)
	sp.OnEnd(stub)
	if got := tr.snapshot(); len(got) != 0 {
		t.Fatalf("after the span end: %v", got)
	}
	// The tracker's Enabled rule: false for weft.messages, true for the
	// rest.
	if tr.Enabled(context.Background(), sdklog.EnabledParameters{EventName: "weft.messages"}) {
		t.Error("tracker answers Enabled(weft.messages) true")
	}
	if !tr.Enabled(context.Background(), sdklog.EnabledParameters{EventName: "weft.event"}) {
		t.Error("tracker answers Enabled(weft.event) false")
	}
}

// Content shaping: caps and redaction on content-on chains, event and
// delta bodies only.
func TestShapeEventCapsAndRedacts(t *testing.T) {
	long := strings.Repeat("x", 100)
	p := &destProc{content: true, contentC: ContentConfig{
		MaxBytes: 10,
		Redact:   func(kind core.ContentKind, s string) string { return strings.ReplaceAll(s, "secret", "[redacted]") },
	}, drops: newDropCounter("test")}
	r := sdkRecordWith(t, "weft.delta",
		`{"type":"text_delta","run_id":"r","text":"secret `+long+`"}`,
		attribute.String("weft.record", "delta"))
	p.shapeEvent(r)
	body := r.Body().AsString()
	if !strings.Contains(body, "[redacted]") {
		t.Errorf("redaction not applied: %s", body)
	}
	if !strings.Contains(body, `"text":"[redacted]"`) {
		t.Errorf("cap not applied: %s", body)
	}
	var truncated bool
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == "weft.content.truncated_bytes" {
			truncated = true
		}
		return true
	})
	if !truncated {
		t.Error("weft.content.truncated_bytes not set after a cap")
	}
	// A non-weft body passes through untouched.
	r2 := sdkRecordWith(t, "", long)
	p.shapeEvent(r2)
	if r2.Body().AsString() != long {
		t.Error("a non-weft body was reshaped")
	}
}

// Strip path: a content-off chain marks records stripped and keeps the
// shape.
func TestStripEventMarksStripped(t *testing.T) {
	p := &destProc{content: false, drops: newDropCounter("test")}
	r := sdkRecordWith(t, "weft.event",
		`{"type":"tool_finish","run_id":"r","seq":1,"call_id":"c","name":"slow","content":"result text"}`,
		attribute.String("weft.record", "event"))
	p.stripEvent(r)
	body := r.Body().AsString()
	if !strings.Contains(body, `"content":""`) {
		t.Errorf("content not stripped: %s", body)
	}
	if !strings.Contains(body, `"name":"slow"`) || !strings.Contains(body, `"seq":1`) {
		t.Errorf("shape not kept: %s", body)
	}
	if attrOf(*r, "weft.content") != "stripped" {
		t.Error("weft.content=stripped not set")
	}
}

// Install with nothing configured records nothing: no destination, one
// INFO line naming how to configure one, a no-op shutdown, no global
// pipeline.
func TestInstallZeroConfigRecordsNothing(t *testing.T) {
	devEnv(t, nil)
	logs := captureSlog(t)
	shutdown := Install()
	defer shutdown()
	if Installed() != nil {
		t.Fatal("Install() with nothing configured registered a pipeline")
	}
	if !strings.Contains(logs.String(), "no destination configured") ||
		!strings.Contains(logs.String(), "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Errorf("Install() INFO line missing or without its hint:\n%s", logs.String())
	}
	if n := strings.Count(logs.String(), "level="); n != 1 {
		t.Errorf("Install() logged %d lines, want 1:\n%s", n, logs.String())
	}
	// Start says why.
	if _, err := Start(testCtx(t), NoGlobal()); !errors.Is(err, ErrNoDestinations) {
		t.Errorf("Start() = %v, want ErrNoDestinations", err)
	}
}

// Fallback: used when nothing else is configured, never beside a named
// destination.
func TestFallback(t *testing.T) {
	devEnv(t, nil)
	fb, named := newMemSink(), newMemSink()
	p, err := Start(testCtx(t), NoGlobal(), Heartbeat(0), Fallback(fb.option()))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = agentThrough(t, p)
	_ = p.Shutdown(testCtx(t))
	if len(fb.eventTypes()) != 8 {
		t.Errorf("fallback received %d events, want 8", len(fb.eventTypes()))
	}
	fb2 := newMemSink()
	p, err = Start(testCtx(t), NoGlobal(), Heartbeat(0), named.option(), Fallback(fb2.option()))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = agentThrough(t, p)
	_ = p.Shutdown(testCtx(t))
	if fb2.opened != 0 || len(named.eventTypes()) != 8 {
		t.Errorf("fallback opened %d times beside a named destination (named got %d events)", fb2.opened, len(named.eventTypes()))
	}
}

// Shutdown is idempotent (heartbeats stop, providers shut down, the
// Sink's exporters close last and only once).
func TestShutdownIdempotent(t *testing.T) {
	sink := newMemSink()
	p, err := Start(testCtx(t), NoGlobal(), NoEnv(), sink.option(), Heartbeat(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = agentThrough(t, p)
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if n := sink.shutdowns(); n != 2 {
		t.Errorf("the Sink's exporters were shut down %d times, want 2 (once each)", n)
	}
}

// The shaping table covers every content class the core's own
// StripContent names (content.go): with Redact configured, steered
// user text and run_finish pending args must not ride unredacted to a
// content-on destination (the programme audit's P1-7 — only ToolStart
// had been adjudicated, and Pending[].Args is the same class:
// redact-not-cap, a cut mid-JSON would make the args undecodable).
// Nested recurses into the child event.
func TestShapeEventRedactsSteeredPendingNested(t *testing.T) {
	red := func(kind core.ContentKind, s string) string {
		return strings.ReplaceAll(s, "secret", "[redacted]")
	}
	p := &destProc{content: true, contentC: ContentConfig{MaxBytes: 40, Redact: red}, drops: newDropCounter("test")}

	shape := func(t *testing.T, body string) core.Event {
		t.Helper()
		r := sdkRecordWith(t, "weft.event", body, attribute.String("weft.record", "event"))
		p.shapeEvent(r)
		ev, err := core.UnmarshalEvent([]byte(r.Body().AsString()))
		if err != nil {
			t.Fatalf("shaped body is not a valid event: %v (%s)", err, r.Body().AsString())
		}
		return ev
	}

	// Steered: the delivered user message's text is redacted (and
	// capped like other user text).
	st := shape(t, `{"type":"steered","run_id":"r","seq":1,"step":0,`+
		`"messages":[{"role":"user","content":[{"type":"text","text":"the secret word"}]}]}`)
	s, ok := st.(core.Steered)
	if !ok || len(s.Messages) != 1 {
		t.Fatalf("steered shape lost: %#v", st)
	}
	tp, ok := s.Messages[0].Content[0].(core.TextPart)
	if !ok || !strings.Contains(tp.Text, "[redacted]") || strings.Contains(tp.Text, "secret") {
		t.Errorf("steered text not redacted: %q", tp.Text)
	}

	// RunFinish: pending args are redacted (adjudicated
	// ToolStart.Args class) and never capped — a 100-byte args
	// document survives the 40-byte cap intact.
	long := `{"note":"` + strings.Repeat("y", 100) + `","secret":"k"}`
	finishBody, err := json.Marshal(core.RunFinish{
		RunID: "r", Steps: 1,
		Pending: []core.ToolCallPart{{ID: "c1", Name: "refund", Args: json.RawMessage(long)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rf := shape(t, string(finishBody))
	f, ok := rf.(core.RunFinish)
	if !ok || len(f.Pending) != 1 {
		t.Fatalf("run_finish shape lost: %#v", rf)
	}
	if args := string(f.Pending[0].Args); !strings.Contains(args, "[redacted]") || strings.Contains(args, "secret") {
		t.Errorf("pending args not redacted: %s", args)
	}
	if len(f.Pending[0].Args) != len(long)-len("secret")+len("[redacted]") {
		t.Errorf("pending args were capped: %d bytes, want the full document", len(f.Pending[0].Args))
	}

	// Nested: the child event is shaped with the same rules.
	nestedBody := `{"type":"nested","run_id":"r","seq":2,"call_id":"c0",` +
		`"event":{"type":"text_delta","run_id":"r","text":"a secret and a good deal more text well past the cap"}}`
	nestedRec := sdkRecordWith(t, "weft.event", nestedBody, attribute.String("weft.record", "event"))
	p.shapeEvent(nestedRec)
	nestedEv, err := core.UnmarshalEvent([]byte(nestedRec.Body().AsString()))
	if err != nil {
		t.Fatalf("shaped nested body invalid: %v", err)
	}
	n, ok := nestedEv.(core.Nested)
	if !ok {
		t.Fatalf("nested shape lost: %#v", nestedEv)
	}
	td, ok := n.Event.(core.TextDelta)
	if !ok || !strings.Contains(td.Text, "[redacted]") || strings.Contains(td.Text, "secret") {
		t.Errorf("nested child not redacted: %q", td.Text)
	}
	if len(td.Text) > 40 {
		t.Errorf("nested child not capped: %q (%d bytes)", td.Text, len(td.Text))
	}
	var truncated bool
	nestedRec.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == "weft.content.truncated_bytes" {
			truncated = true
		}
		return true
	})
	if !truncated {
		t.Error("the nested cut did not record weft.content.truncated_bytes")
	}
}
