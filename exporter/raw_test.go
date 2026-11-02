package exporter

// RawBodies' records through the Local sink into the store (moved from
// weft/otel's raw_test.go, which pins the same rule against an
// in-memory Sink): obsdb reads each weft.raw as the attempt's Wire.

import (
	"context"
	"iter"
	"path/filepath"
	"strings"
	"testing"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/core/wefttest"
	"github.com/weftgo/weft/obsdb"
	"github.com/weftgo/weft/otel"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

func recordsOfKind(recs []sdklog.Record, kind string) []sdklog.Record {
	var out []sdklog.Record
	for _, r := range recs {
		if attrOf(r, "weft.record") == kind {
			out = append(out, r)
		}
	}
	return out
}

// rawReporter is a model middleware that asks WantsRaw and reports one
// exchange per call, the way a first-party adapter's capture does.
type rawReporter struct {
	next   core.Model
	wanted *bool
}

func (m rawReporter) Stream(ctx context.Context, req core.ModelRequest) iter.Seq2[core.ModelEvent, error] {
	r := core.ReportFromContext(ctx)
	*m.wanted = r.WantsRaw()
	r.Raw(core.RawPair{
		Method: "POST", URL: "https://api.example/v1/messages",
		Request: []byte(`{"q":"wire"}`), MediaType: "application/json",
		RequestHeaders: map[string][]string{"Authorization": {"Bearer SECRET"}},
		Status:         200, Response: []byte("data: ok\n\n"), ResponseMediaType: "text/event-stream",
	})
	return m.next.Stream(ctx, req)
}

// rawThrough runs one step through p reporting one exchange; it returns
// what WantsRaw said and the run id.
func rawThrough(t *testing.T, p *otel.Pipeline) (bool, string) {
	t.Helper()
	var wanted bool
	agt := core.New(wefttest.Script(wefttest.Say("done")),
		core.WrapModel(func(next core.Model) core.Model { return rawReporter{next: next, wanted: &wanted} }),
		core.TracerProvider(p.TracerProvider()), core.LoggerProvider(p.LoggerProvider()))
	res, err := agt.Generate(context.Background(), core.Prompt("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return wanted, res.ID
}

// End to end under dev: the Local sink stores the raw record, a
// content-on Exporters destination beside it receives none (counted as
// filtered), and the record shows on the Local side only.
func TestRawBodiesNeverToThirdParty(t *testing.T) {
	t.Setenv("WEFT_ENV", "dev")
	isolate(t)
	mem := newMemExporter()
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.Heartbeat(0), otel.RawBodies(true),
		Local(filepath.Join(t.TempDir(), "weft.db")), otel.Exporters(nil, mem, otel.WithContent()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	wanted, id := rawThrough(t, p)
	if !wanted {
		t.Fatal("WantsRaw = false with a Local sink under RawBodies and dev")
	}
	if n := len(recordsOfKind(mem.snapshot(), "raw")); n != 0 {
		t.Errorf("the Exporters destination received %d raw records", n)
	}
	if len(recordsOfKind(mem.snapshot(), "request")) != 1 {
		t.Error("the Exporters destination lost its request record")
	}
	reqs, err := LocalDBOf(p).Requests(context.Background(), id, obsdb.RequestQuery{})
	if err != nil || len(reqs) != 1 || reqs[0].Wire == nil {
		t.Fatalf("local requests = %+v, %v; want one with its raw record", reqs, err)
	}
	w := reqs[0].Wire
	if w.Request.Body != `{"q":"wire"}` || w.Response.Body != "data: ok\n\n" || w.RedactedHeaders[0] != "authorization" {
		t.Errorf("stored wire = %+v / %+v", w.Request, w.Response)
	}
}

// A content-on destination's Redact sees each raw body as
// core.ContentRaw; a base64 body is left alone.
func TestRawBodiesRedact(t *testing.T) {
	t.Setenv("WEFT_ENV", "dev")
	isolate(t)
	var kinds []core.ContentKind
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.Heartbeat(0), otel.RawBodies(true),
		Local(filepath.Join(t.TempDir(), "weft.db"), otel.WithContent(otel.ContentConfig{Redact: func(k core.ContentKind, s string) string {
			if k == core.ContentRaw {
				kinds = append(kinds, k)
				return strings.ReplaceAll(s, "wire", "[X]")
			}
			return s
		}})))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_, id := rawThrough(t, p)
	reqs, err := LocalDBOf(p).Requests(context.Background(), id, obsdb.RequestQuery{})
	if err != nil || len(reqs) != 1 || reqs[0].Wire == nil {
		t.Fatalf("requests = %+v, %v", reqs, err)
	}
	if reqs[0].Wire.Request.Body != `{"q":"[X]"}` || len(kinds) != 2 {
		t.Errorf("redacted body = %q, Redact calls = %d", reqs[0].Wire.Request.Body, len(kinds))
	}
}

// The global path: exporter.Install's zero-config local sink registered
// globally and an agent with no LoggerProvider option. Under dev with
// RawBodies the marker is reached through the registered global
// provider (the core re-reads the global at each ask) and the raw
// record lands in the sink. The agent is built after Install: an
// earlier test in this process may have left its own (shut-down)
// provider registered, which an agent built then would emit to.
func TestRawBodiesThroughTheGlobalProvider(t *testing.T) {
	t.Setenv("WEFT_ENV", "dev")
	isolate(t)
	t.Setenv("WEFT_DB", filepath.Join(t.TempDir(), "weft.db"))
	shutdown := Install(otel.RawBodies(true), otel.Heartbeat(0))
	defer shutdown()
	var wanted bool
	agt := core.New(wefttest.Script(wefttest.Say("done")),
		core.WrapModel(func(next core.Model) core.Model { return rawReporter{next: next, wanted: &wanted} }))
	res, err := agt.Generate(context.Background(), core.Prompt("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if !wanted {
		t.Fatal("WantsRaw = false through the globally installed pipeline")
	}
	db := LocalDB()
	if db == nil {
		t.Fatal("no local sink installed")
	}
	reqs, err := db.Requests(context.Background(), res.ID, obsdb.RequestQuery{})
	if err != nil || len(reqs) != 1 || reqs[0].Wire == nil || reqs[0].Wire.Request.Body != `{"q":"wire"}` {
		t.Fatalf("requests = %+v, %v; want one with its raw record", reqs, err)
	}
}
