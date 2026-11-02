package anthropic

// Plan A6's Done line for the anthropic adapter: under
// otel.RawBodies(true) and WEFT_ENV=dev, a step's wire pair reaches a
// first-party destination byte for byte, and no credential the SDK (or
// a gateway in front of it) sent reaches any exported byte. (The
// platform's twin pins the store's round trip.)

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

	antsdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/otel"
	"go.opentelemetry.io/otel/attribute"
)

// rawEnv is the development environment RawBodies needs, with the
// environment destination switched off, so the pipeline is exactly the
// test's capture.
func rawEnv(t *testing.T) {
	t.Setenv("WEFT_ENV", "dev")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
}

// wireServer serves the SSE fixture with response headers that include
// a cookie, and records the request exactly as it arrived.
type wireServer struct {
	*httptest.Server
	sse    []byte
	mu     sync.Mutex
	body   []byte
	header http.Header
}

func newWireServer(t *testing.T) *wireServer {
	t.Helper()
	sse, err := os.ReadFile("testdata/text_only.sse")
	if err != nil {
		t.Fatal(err)
	}
	ws := &wireServer{sse: sse}
	ws.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ws.mu.Lock()
		ws.body, ws.header = b, r.Header.Clone()
		ws.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Request-Id", "req_wire_1")
		w.Header().Set("Set-Cookie", "session=SECRET-set-cookie")
		_, _ = w.Write(sse)
	}))
	t.Cleanup(ws.Close)
	return ws
}

// rawRound runs one step of an anthropic model on c under a
// first-party capture with RawBodies, and returns the attempt's raw
// record (nil without one), its request record's body and every byte
// the destination was handed.
func rawRound(t *testing.T, c antsdk.Client) (*rawWire, string, []byte) {
	t.Helper()
	ctx := context.Background()
	capture := &rawCapture{}
	p, err := otel.Start(ctx, otel.NoGlobal(), otel.Exporters(nil, capture, otel.FirstParty()), otel.RawBodies(true))
	if err != nil {
		t.Fatal(err)
	}
	agt := core.New(Model("claude-test", Client(&c)),
		core.LoggerProvider(p.LoggerProvider()), core.TracerProvider(p.TracerProvider()))
	if _, err := agt.Generate(ctx, core.Prompt("hi")); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	reqs, _ := capture.of(t, "request")
	if len(reqs) != 1 {
		t.Fatalf("request records = %d, want 1", len(reqs))
	}
	w, exported := capture.wire(t)
	return w, reqs[0], exported
}

// stripList is ADR 0028 §12's header floor plus anthropic-sdk-go's own
// sensitive list.
var stripList = []string{
	"authorization", "proxy-authorization", "x-api-key", "api-key",
	"cookie", "set-cookie", "x-goog-api-key",
}

func assertNoStripped(t *testing.T, side string, h map[string][]string) {
	t.Helper()
	for k := range h {
		for _, s := range stripList {
			if strings.EqualFold(k, s) {
				t.Errorf("%s header %q survived", side, k)
			}
		}
	}
}

func TestRawBodiesRoundTrip(t *testing.T) {
	rawEnv(t)
	ws := newWireServer(t)
	c := antsdk.NewClient(option.WithBaseURL(ws.URL), option.WithAPIKey("SECRET-api-key"),
		option.WithAuthToken("SECRET-auth-token"), option.WithMaxRetries(0),
		option.WithHeader("Cookie", "c=SECRET-cookie"),
		option.WithHeader("Proxy-Authorization", "Basic SECRET-proxy"))
	w, _, stored := rawRound(t, c)
	if w == nil {
		t.Fatal("the attempt's request record carries no raw record")
	}
	ws.mu.Lock()
	sent, got := ws.body, ws.header
	ws.mu.Unlock()
	if w.Request.Body != string(sent) || w.Request.Encoding != "" {
		t.Errorf("stored request body differs from what the server received:\n got %q\nwant %q", w.Request.Body, sent)
	}
	if w.Response == nil || w.Response.Body != string(ws.sse) {
		t.Fatalf("stored response body differs from what the server sent: %+v", w.Response)
	}
	if w.Request.Method != http.MethodPost || !strings.HasSuffix(w.Request.URL, "/v1/messages") ||
		w.Request.MediaType != "application/json" || w.Response.Status != 200 ||
		w.Response.MediaType != "text/event-stream" || w.Step != 0 || w.Attempt != 1 {
		t.Errorf("wire = %s %s (%s) → %d (%s), step %d attempt %d", w.Request.Method, w.Request.URL,
			w.Request.MediaType, w.Response.Status, w.Response.MediaType, w.Step, w.Attempt)
	}
	if w.Response.Headers["Request-Id"][0] != "req_wire_1" || w.Request.Headers["Anthropic-Version"] == nil {
		t.Errorf("kept headers = %v / %v", w.Request.Headers, w.Response.Headers)
	}
	assertNoStripped(t, "request", w.Request.Headers)
	assertNoStripped(t, "response", w.Response.Headers)
	for _, h := range []string{"authorization", "cookie", "proxy-authorization", "set-cookie", "x-api-key"} {
		listed := false
		for _, r := range w.RedactedHeaders {
			listed = listed || r == h
		}
		if !listed {
			t.Errorf("redacted_headers %v does not name %s", w.RedactedHeaders, h)
		}
	}
	// The server did receive the credentials: the capture stripped them
	// from the record, never from the request.
	if got.Get("X-Api-Key") != "SECRET-api-key" || got.Get("Authorization") != "Bearer SECRET-auth-token" {
		t.Errorf("the request lost its credentials: %v", got)
	}
	if bytes.Contains(stored, []byte("SECRET")) {
		t.Error("a credential reached the destination")
	}
}

// Every case variant of every credential header the SDKs send, times
// every value shape — set on the wire by a client middleware outside the
// adapter's, so non-canonical spellings reach the capture as written —
// plus Bearer/Basic values under an innocuous name: the header is
// stripped from the exported record and named in redacted_headers, and
// its value is in no byte the destination was handed nor in the request
// record. Fuzz-seeded: the seeds are FuzzRawRedaction's.
func TestRawBodiesNoCredentialStored(t *testing.T) {
	rawEnv(t)
	names := []string{
		"authorization", "AUTHORIZATION", "Authorization", "X-API-Key", "x-api-key", "X-Api-Key",
		"api-key", "Api-Key", "Proxy-Authorization", "cookie", "COOKIE", "x-goog-api-key",
		"X-Goog-Api-Key", "X-Auth-Token", "X-Client-Secret",
	}
	shapes := []func(id string) string{
		func(id string) string { return "SECRET-" + id },
		func(id string) string { return "Bearer SECRET-" + id },
		func(id string) string { return "Basic U0VDUkVU" + id },
		func(id string) string { return `SECRET-"q"<&>` + id },
	}
	type tc struct{ name, value string }
	var cases []tc
	for _, n := range names {
		for i, shape := range shapes {
			cases = append(cases, tc{n, shape(strings.ToLower(n) + "-" + string(rune('a'+i)))})
		}
	}
	for _, v := range []string{"Bearer SECRET-trace", "basic SECRET-trace2", "BEARER SECRET-trace3"} {
		cases = append(cases, tc{"X-Trace", v})
	}
	for _, c := range cases {
		t.Run(c.name+"/"+c.value, func(t *testing.T) {
			ws := newWireServer(t)
			cl := antsdk.NewClient(option.WithBaseURL(ws.URL), option.WithAPIKey("SECRET-key"), option.WithMaxRetries(0),
				option.WithMiddleware(func(r *http.Request, next option.MiddlewareNext) (*http.Response, error) {
					r.Header[c.name] = []string{c.value}
					return next(r)
				}))
			w, req, stored := rawRound(t, cl)
			if w == nil {
				t.Fatal("no raw record")
			}
			assertNoStripped(t, "request", w.Request.Headers)
			for k := range w.Request.Headers {
				if strings.EqualFold(k, c.name) {
					t.Errorf("%q survived as %q", c.name, k)
				}
			}
			listed := false
			for _, r := range w.RedactedHeaders {
				listed = listed || r == strings.ToLower(c.name)
			}
			if !listed {
				t.Errorf("redacted_headers %v does not name %q", w.RedactedHeaders, c.name)
			}
			esc, _ := json.Marshal(c.value)
			for _, needle := range [][]byte{[]byte(c.value), esc[1 : len(esc)-1], []byte("SECRET-key")} {
				if bytes.Contains(stored, needle) {
					t.Errorf("%q reached the destination", needle)
				}
			}
			b, _ := json.Marshal(w)
			b = append(b, req...)
			if bytes.Contains(b, []byte("SECRET")) || bytes.Contains(b, []byte("U0VDUkVU")) {
				t.Errorf("the raw or request record carries a credential: %s", b)
			}
		})
	}
}

// Outside WEFT_ENV=dev the option is a no-op: the adapter's capture
// asks and is told no, the request goes out untouched, and the run
// exports no raw record.
func TestRawBodiesOffOutsideDev(t *testing.T) {
	rawEnv(t)
	t.Setenv("WEFT_ENV", "")
	ws := newWireServer(t)
	var asked, wanted bool
	c := antsdk.NewClient(option.WithBaseURL(ws.URL), option.WithAPIKey("k"), option.WithMaxRetries(0),
		option.WithMiddleware(func(r *http.Request, next option.MiddlewareNext) (*http.Response, error) {
			asked, wanted = true, core.ReportFromContext(r.Context()).WantsRaw()
			return next(r)
		}))
	w, _, _ := rawRound(t, c)
	if !asked || wanted {
		t.Errorf("asked %v, WantsRaw %v; want the adapter told no", asked, wanted)
	}
	if w != nil {
		t.Errorf("a raw record was exported outside dev: %+v", w)
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
