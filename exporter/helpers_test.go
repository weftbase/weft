package exporter

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/obsdb"
	"github.com/weftgo/weft/obsdb/sqlite"
	"github.com/weftgo/weft/otel"
	"github.com/weftgo/weft/wefttest"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

func testCtx(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

// sqliteOpen reopens a Local sink file after Shutdown closed it.
func sqliteOpen(path string) (obsdb.DB, error) { return sqlite.Open(path) }

// isolate keeps a test from the operator's Studio: no discovery file,
// no WEFT_STUDIO_URL; every other variable reads through (t.Setenv).
func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	prev := envGetenv
	envGetenv = func(k string) string {
		switch k {
		case "WEFT_DISCOVERY":
			return "off"
		case "WEFT_STUDIO_URL", "WEFT_STUDIO_TOKEN":
			return ""
		}
		return os.Getenv(k)
	}
	t.Cleanup(func() { envGetenv = prev })
}

// memExporter is an in-memory log exporter.
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

// agentThrough runs one scripted agent run against the pipeline's
// providers (hermetic: NoGlobal, providers passed explicitly). The run
// is two steps (a tool step and a final say), so its durable events
// are run_start, step_start, tool_start, tool_finish, step_finish,
// step_start, step_finish, run_finish — eight.
func agentThrough(t *testing.T, p *otel.Pipeline) (*core.RunResult, string) {
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
