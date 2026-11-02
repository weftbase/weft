package google

// The wire capture (ADR 0028 §12, plan A6) for this adapter: under
// otel.RawBodies(true) and WEFT_ENV=dev a step's exchange reaches a
// first-party destination byte for byte, with no credential in it.
// (The anthropic adapter's raw_test.go holds the case-variant table.)

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	sdklog "go.opentelemetry.io/otel/sdk/log"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/otel"
	"go.opentelemetry.io/otel/attribute"
)

func TestRawBodiesRoundTrip(t *testing.T) {
	t.Setenv("WEFT_ENV", "dev")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	sse, err := os.ReadFile("testdata/text_only.sse")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu   sync.Mutex
		sent []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		sent = b
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Set-Cookie", "s=SECRET-cookie")
		_, _ = w.Write(sse)
	}))
	defer srv.Close()

	ctx := context.Background()
	capture := &rawCapture{}
	p, err := otel.Start(ctx, otel.NoGlobal(), otel.Exporters(nil, capture, otel.FirstParty()), otel.RawBodies(true))
	if err != nil {
		t.Fatal(err)
	}
	// The self-built client (BaseURL + APIKey): its transport is the one
	// the adapter wraps. The loopback server is the only destination.
	t.Setenv("WEFT_MODEL_REQUESTS", "")
	t.Setenv("GOOGLE_GENAI_USE_VERTEXAI", "")
	m := Model("gemini-test", BaseURL(srv.URL), APIKey("SECRET-goog"))
	agt := core.New(m, core.LoggerProvider(p.LoggerProvider()), core.TracerProvider(p.TracerProvider()))
	res, err := agt.Generate(ctx, core.Prompt("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	_ = res
	if reqs, _ := capture.of(t, "request"); len(reqs) != 1 {
		t.Fatalf("request records = %d, want 1", len(reqs))
	}
	w, exported := capture.wire(t)
	if w == nil {
		t.Fatal("no raw record")
	}
	mu.Lock()
	defer mu.Unlock()
	if w.Request.Body != string(sent) || w.Response == nil || w.Response.Body != string(sse) {
		t.Errorf("stored exchange differs from the wire:\nrequest %q\n   sent %q", w.Request.Body, sent)
	}
	for _, side := range []map[string][]string{w.Request.Headers, w.Response.Headers} {
		for k := range side {
			for _, s := range []string{"authorization", "x-goog-api-key", "api-key", "set-cookie", "cookie"} {
				if strings.EqualFold(k, s) {
					t.Errorf("header %q survived", k)
				}
			}
		}
	}
	if len(w.RedactedHeaders) == 0 {
		t.Error("redacted_headers names nothing")
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(exported, []byte("SECRET")) {
		t.Error("a credential reached the destination")
	}
}

// rawCapture is the test's first-party destination: an in-memory log
// exporter marked otel.FirstParty, so RawBodies' weft.raw records reach
// it (a store's round trip of the same records is the platform's test).
type rawCapture struct {
	mu   sync.Mutex
	recs []sdklog.Record
}

func (c *rawCapture) Export(_ context.Context, recs []sdklog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range recs {
		c.recs = append(c.recs, r.Clone())
	}
	return nil
}

func (c *rawCapture) Shutdown(context.Context) error   { return nil }
func (c *rawCapture) ForceFlush(context.Context) error { return nil }

// rawWire is a weft.raw record's body (ADR 0028 §12).
type rawWire struct {
	Step            int          `json:"step"`
	Attempt         int64        `json:"attempt"`
	Request         rawWireSide  `json:"request"`
	Response        *rawWireSide `json:"response"`
	RedactedHeaders []string     `json:"redacted_headers"`
}

type rawWireSide struct {
	Method    string              `json:"method"`
	URL       string              `json:"url"`
	Status    int                 `json:"status"`
	Headers   map[string][]string `json:"headers"`
	MediaType string              `json:"media_type"`
	Body      string              `json:"body"`
	Encoding  string              `json:"encoding"`
}

// of returns the bodies of the records of kind (weft.record) and every
// byte the destination was handed: each record's body and attribute
// values.
func (c *rawCapture) of(t *testing.T, kind string) (bodies []string, exported []byte) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.recs {
		exported = append(exported, r.Body().AsString()...)
		var k string
		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			exported = append(exported, kv.Value.Emit()...)
			if kv.Key == "weft.record" {
				k = kv.Value.AsString()
			}
			return true
		})
		if k == kind {
			bodies = append(bodies, r.Body().AsString())
		}
	}
	return bodies, exported
}

// wire decodes the run's one raw record; nil when there is none.
func (c *rawCapture) wire(t *testing.T) (*rawWire, []byte) {
	t.Helper()
	bodies, exported := c.of(t, "raw")
	if len(bodies) == 0 {
		return nil, exported
	}
	if len(bodies) != 1 {
		t.Fatalf("raw records = %d, want 1", len(bodies))
	}
	var w rawWire
	if err := json.Unmarshal([]byte(bodies[0]), &w); err != nil {
		t.Fatalf("raw record body: %v", err)
	}
	return &w, exported
}
