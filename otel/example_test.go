package otel_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"

	"github.com/weftgo/weft"
	"github.com/weftgo/weft/mw"
	"github.com/weftgo/weft/otel"
	"github.com/weftgo/weft/wefttest"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// memLogs is an in-memory log exporter: the examples' stand-in for a
// store or a collector.
type memLogs struct {
	mu   sync.Mutex
	recs []sdklog.Record
}

func (m *memLogs) Export(_ context.Context, recs []sdklog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range recs {
		m.recs = append(m.recs, r.Clone())
	}
	return nil
}

func (m *memLogs) Shutdown(context.Context) error   { return nil }
func (m *memLogs) ForceFlush(context.Context) error { return nil }

// attr reads one string attribute off a record.
func attr(r sdklog.Record, key string) string {
	var out string
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			out = kv.Value.Emit()
			return false
		}
		return true
	})
	return out
}

// appLogs is the records the slog bridge teed: they carry no weft
// record kind.
func (m *memLogs) appLogs() []sdklog.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sdklog.Record
	for _, r := range m.recs {
		if attr(r, "weft.record") == "" && r.EventName() == "" {
			out = append(out, r)
		}
	}
	return out
}

// Install is the whole setup: it registers the OTel globals the core
// reads, never fails the program, and returns the shutdown to call on
// exit. NoEnv keeps the example hermetic — no OTEL_* variable can add a
// destination behind it. FirstParty marks the exporters as the
// operator's own: content on.
func ExampleInstall() {
	logs := &memLogs{}
	shutdown := otel.Install(
		otel.NoEnv(),
		otel.Exporters(nil, logs, otel.FirstParty()),
	)

	// A scripted run through the installed globals — a real program
	// wires nothing: the core reads the providers Install registered.
	agt := weft.New(
		wefttest.Script(wefttest.Say("all done")),
		weft.Name("example"),
	)
	if _, err := agt.Generate(context.Background(), weft.Prompt("go"),
		weft.Metadata(map[string]string{"weft.session.id": "example-session"})); err != nil {
		log.Fatal(err)
	}
	shutdown() // flush every destination

	// The run's durable events, and the transcript the messages records
	// hold.
	var events []string
	messages := 0
	for _, r := range logs.recs {
		switch attr(r, "weft.record") {
		case "event":
			events = append(events, attr(r, "weft.event.type"))
		case "messages":
			messages += strings.Count(r.Body().AsString(), `"role"`)
		}
	}
	fmt.Println(strings.Join(events, " "), "·", messages, "messages")
	// Output: run_start step_start step_finish run_finish · 2 messages
}

// Slog bridges slog.Default: weft's own Debug run lines (weft.Logger's
// default is slog.Default) and mw.Log's (whose nil logger is too) reach
// the pipeline's first-party destinations, each under the span it was
// logged in — a viewer's app logs for the run.
func ExampleSlog() {
	logs := &memLogs{}
	shutdown := otel.Install(otel.NoEnv(), otel.Exporters(nil, logs, otel.FirstParty()), otel.Slog())

	agt := weft.New(wefttest.Script(wefttest.Say("all done")),
		weft.Name("example"), weft.WrapModel(mw.Log(nil)))
	if _, err := agt.Generate(context.Background(), weft.Prompt("go")); err != nil {
		log.Fatal(err)
	}
	shutdown() // flushes, and puts slog.Default back

	for _, l := range logs.appLogs() {
		fmt.Println(l.Severity(), l.Body().AsString())
	}
	// Output:
	// DEBUG run start
	// DEBUG model request
	// DEBUG model finish
	// DEBUG model call
	// DEBUG run finish
}

// SlogHandler bridges a logger the program builds itself: here a tool
// handler's WARN, logged with the tool call's context, is exported
// under that call's execute_tool span — the line and the step it came
// from, joined. The handler follows the installed pipeline, so it may be
// built before Install.
func ExampleSlogHandler() {
	logger := slog.New(otel.SlogHandler(slog.NewTextHandler(io.Discard, nil)))
	spans, logs := tracetest.NewInMemoryExporter(), &memLogs{}
	shutdown := otel.Install(otel.NoEnv(), otel.Exporters(spans, logs, otel.FirstParty()))

	lookup := weft.Tool("lookup", "Look an order up.", func(ctx context.Context, in struct {
		ID string `json:"id"`
	}) (string, error) {
		logger.WarnContext(ctx, "slow lookup", "id", in.ID)
		return "shipped", nil
	})
	agt := weft.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "lookup", Args: `{"id":"42"}`}),
		wefttest.Say("shipped")), weft.Name("example"), lookup)
	if _, err := agt.Generate(context.Background(), weft.Prompt("where is 42?")); err != nil {
		log.Fatal(err)
	}
	// Flush before reading: the in-memory span exporter forgets its
	// spans at shutdown.
	if err := otel.Installed().ForceFlush(context.Background()); err != nil {
		log.Fatal(err)
	}
	names := map[string]string{}
	for _, s := range spans.GetSpans() {
		names[s.SpanContext.SpanID().String()] = s.Name
	}
	shutdown()
	for _, l := range logs.appLogs() {
		fmt.Println(l.Severity(), l.Body().AsString(), "·", names[l.SpanID().String()], "· id", attr(l, "id"))
	}
	// Output: WARN slow lookup · execute_tool lookup · id 42
}

// ContentLog is the kind Redact sees for a bridged app log line: its
// message and each string attribute are content like a transcript
// field, redacted (then capped at MaxBytes) before they leave — here a
// card number a tool handler logged never reaches the destination.
func ExampleContentLog() {
	redact := func(kind weft.ContentKind, s string) string {
		if kind == otel.ContentLog {
			return strings.ReplaceAll(s, "4111111111111111", "[card]")
		}
		return s
	}
	logs := &memLogs{}
	shutdown := otel.Install(otel.NoEnv(), otel.Exporters(nil, logs, otel.FirstParty()),
		otel.Content(otel.ContentConfig{Redact: redact}))
	logger := slog.New(otel.SlogHandler(nil))

	charge := weft.Tool("charge", "Charge a card.", func(ctx context.Context, in struct {
		Card string `json:"card"`
	}) (string, error) {
		logger.InfoContext(ctx, "charging "+in.Card, "card", in.Card)
		return "ok", nil
	})
	agt := weft.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "charge", Args: `{"card":"4111111111111111"}`}),
		wefttest.Say("charged")), weft.Name("example"), charge)
	if _, err := agt.Generate(context.Background(), weft.Prompt("charge it")); err != nil {
		log.Fatal(err)
	}
	shutdown()

	for _, l := range logs.appLogs() {
		fmt.Println(l.Body().AsString(), "· card", attr(l, "card"))
	}
	// Output: charging [card] · card [card]
}
