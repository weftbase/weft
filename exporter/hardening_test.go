package exporter

// The Local sink's hardening (moved from weft/otel's hardening_test.go):
// every database closes, the package accessors clear at shutdown, a
// write blocked behind SQLite's lock never holds Shutdown past its
// context, a failing database never floods OTel's error handler, and
// the SDK-to-obsdb attribute mapping keeps every type.

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/core/wefttest"
	"github.com/weftgo/weft/otel"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// openFilesUnder counts this process's open descriptors on files under
// dir (Linux's /proc; the test skips elsewhere).
func openFilesUnder(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd on this platform")
	}
	var n int
	for _, e := range entries {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(target, dir) {
			n++
		}
	}
	return n
}

// goroutineDump is every goroutine's stack.
func goroutineDump() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

type threadSafeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// errorHandlerFunc counts what reaches OTel's global error handler.
type errorHandlerFunc func(error)

func (f errorHandlerFunc) Handle(err error) { f(err) }

// Every Local destination's database closes at Shutdown, not only the
// last one configured; and a Start that fails after opening one closes
// it.
func TestEveryLocalDBCloses(t *testing.T) {
	dir := t.TempDir()
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(),
		Local(filepath.Join(dir, "a.db")),
		Local(filepath.Join(dir, "b.db")),
		otel.Heartbeat(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if openFilesUnder(t, dir) == 0 {
		t.Fatal("no open database files while running (test bug)")
	}
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if n := openFilesUnder(t, dir); n != 0 {
		t.Errorf("%d database descriptors still open after Shutdown with two Local destinations", n)
	}

	// The half-built path: a Local with neither signal opened its DB
	// before failing.
	dir2 := t.TempDir()
	if _, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(),
		Local(filepath.Join(dir2, "c.db"), otel.Signals(false, false))); err == nil {
		t.Fatal("a destination with neither signal built")
	}
	if n := openFilesUnder(t, dir2); n != 0 {
		t.Errorf("%d database descriptors still open after a failed Start", n)
	}
}

// The package accessors stop handing out a shut-down pipeline's closed
// database and dead endpoint.
func TestPackageAccessorsClearOnShutdown(t *testing.T) {
	isolate(t)
	shutdown := Install(otel.NoEnv(),
		Local(filepath.Join(t.TempDir(), "acc.db")),
		Studio("https://studio.invalid", "tok", otel.Signals(true, false)),
	)
	if LocalDB() == nil {
		shutdown()
		t.Fatal("LocalDB() is nil while installed")
	}
	if u, tok := StudioEndpoint(); u != "https://studio.invalid" || tok != "tok" {
		shutdown()
		t.Fatalf("StudioEndpoint() = %q, %q while installed", u, tok)
	}
	shutdown()
	if db := LocalDB(); db != nil {
		t.Error("LocalDB() still returns the closed database after shutdown")
	}
	if u, _ := StudioEndpoint(); u != "" {
		t.Errorf("StudioEndpoint() = %q after shutdown", u)
	}
}

// Shutdown is bounded by its context while a heartbeat is stuck in a
// destination. The heartbeat goroutine emits one record per open run,
// each through the Local sink's synchronous write, and Shutdown waited
// for it with no bound: stopHeartbeat's <-p.beatDone ignored ctx, and
// the pass ran to its end whatever stop said. With the database's write
// lock held elsewhere (another process: Studio's delete, weft vacuum —
// here a second connection), each heartbeat write waits out SQLite's
// 30 s busy timeout, so `defer exporter.Install()()` (a 5 s context) hung
// for 30 s per open run; under concurrent runs on a slow disk the same
// wait, a pass of open-runs × queued writes, is what blew the CI test
// above's 20 s. Shutdown must return on its context, and a second
// Shutdown returns the first's result.
func TestShutdownBoundedWhileHeartbeatBlocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hb.db")
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(), Local(path), otel.Heartbeat(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	entered := make(chan struct{}, 3)
	hold := core.Tool("hold", "Holds the run open.", func(context.Context, struct{}) (string, error) {
		entered <- struct{}{}
		<-release
		return "ok", nil
	})
	const open = 3
	var runs sync.WaitGroup
	defer runs.Wait()
	defer unblock()
	for range open {
		runs.Add(1)
		go func() {
			defer runs.Done()
			agt := core.New(
				wefttest.Script(wefttest.ToolCalls(wefttest.Call{Name: "hold", Args: `{}`}), wefttest.Say("done")),
				core.TracerProvider(p.TracerProvider()),
				core.LoggerProvider(p.LoggerProvider()),
				hold,
			)
			_, _ = agt.Generate(context.Background(), core.Prompt("go"))
		}()
	}
	for range open {
		<-entered
	}

	// Another connection takes the database's write lock: every Local
	// write now waits in SQLite's busy handler.
	locker, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Close() }()
	conn, err := locker.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()

	// Wait until the heartbeat goroutine is inside the Local chain's
	// synchronous processor: blocked there until the lock goes.
	deadline := time.Now().Add(10 * time.Second)
	for !heartbeatInLocalWrite(goroutineDump()) {
		if time.Now().After(deadline) {
			t.Fatalf("the heartbeat never reached the Local write (test bug)\n%s", goroutineDump())
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Shutdown(ctx) }()
	var first error
	select {
	case first = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Shutdown with a 200ms context blocked on a heartbeat stuck in a destination\n%s", goroutineDump())
	}
	second := make(chan error, 1)
	go func() { second <- p.Shutdown(context.Background()) }()
	select {
	case err := <-second:
		if !errors.Is(err, first) && (err != nil || first != nil) {
			t.Errorf("second Shutdown = %v, want the first's result %v", err, first)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a second Shutdown waited after the first returned\n%s", goroutineDump())
	}
}

// heartbeatInLocalWrite reports whether a goroutine in dump is
// emitting a heartbeat through a synchronous (Local) processor.
func heartbeatInLocalWrite(dump string) bool {
	for _, g := range strings.Split(dump, "\n\n") {
		if strings.Contains(g, "otel.heartbeats(") && strings.Contains(g, "(*SimpleProcessor).OnEmit") {
			return true
		}
	}
	return false
}

// A failing local database does not flood the process's error output:
// every lost record is counted and the throttled WARN names the cause;
// the write error is not also handed to OTel's global error handler —
// one stderr line per record, per delta, for as long as the disk is
// full.
func TestLocalWriteFailureIsCountedNotFlooded(t *testing.T) {
	buf := &threadSafeBuffer{}
	prevLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	defer slog.SetDefault(prevLog)
	var handled atomic.Int64
	prev := otelapi.GetErrorHandler()
	otelapi.SetErrorHandler(errorHandlerFunc(func(error) { handled.Add(1) }))
	defer otelapi.SetErrorHandler(prev)

	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(),
		Local(filepath.Join(t.TempDir(), "gone.db")), otel.Heartbeat(0))
	if err != nil {
		t.Fatal(err)
	}
	_ = LocalDBOf(p).Close() // the sink fails from here on
	agt := core.New(
		wefttest.Script(wefttest.Say("x")),
		core.TracerProvider(p.TracerProvider()),
		core.LoggerProvider(p.LoggerProvider()),
	)
	if _, err := agt.Generate(context.Background(), core.Prompt("go")); err != nil {
		t.Fatalf("a failing sink failed the run: %v", err)
	}
	_ = p.Shutdown(testCtx(t))
	if n := handled.Load(); n != 0 {
		t.Errorf("%d write errors reached OTel's global error handler (one line each on stderr)", n)
	}
	out := buf.String()
	if strings.Count(out, "destination dropped records") != 1 || !strings.Contains(out, "dest=local") {
		t.Errorf("want exactly one throttled WARN for the local sink, got: %s", out)
	}
	if !strings.Contains(out, "err=") {
		t.Errorf("the WARN does not name the cause: %s", out)
	}
}

// The Local path keeps the attribute types the OTLP path keeps (S3.3:
// both make identical rows): bytes → base64, slices → []any, maps →
// map[string]any. They were dropped here and stored by Studio ingest.
func TestAttrValueStructuredTypes(t *testing.T) {
	cases := []struct {
		name string
		in   attribute.Value
		want any
	}{
		{"bytes", attribute.ByteSliceValue([]byte("hi")), "aGk="},
		{"slice", attribute.SliceValue(attribute.StringValue("a"), attribute.Int64Value(2)), []any{"a", int64(2)}},
		{"map", attribute.MapValue(attribute.String("k", "v"), attribute.Bool("b", true)),
			map[string]any{"k": "v", "b": true}},
		{"nested", attribute.MapValue(attribute.KeyValue{Key: "l", Value: attribute.SliceValue(attribute.Float64Value(1.5))}),
			map[string]any{"l": []any{1.5}}},
	}
	for _, c := range cases {
		if got := attrValue(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: attrValue = %#v, want %#v", c.name, got, c.want)
		}
	}
}

// A non-string body (a stock bridge's structured record) renders as
// JSON through the attribute mapping — what obsdb.FromOTLPLogs stores
// for the same body — not as "".
func TestBodyStringStructuredBodies(t *testing.T) {
	for _, c := range []struct {
		name string
		in   attribute.Value
		want string
	}{
		{"string", attribute.StringValue(`{"type":"run_start"}`), `{"type":"run_start"}`},
		{"empty", attribute.Value{}, ""},
		{"int", attribute.Int64Value(7), "7"},
		{"bool", attribute.BoolValue(true), "true"},
		{"double", attribute.Float64Value(1.5), "1.5"},
		{"bytes", attribute.ByteSliceValue([]byte("hi")), "aGk="},
		{"map", attribute.MapValue(attribute.String("msg", "hello"), attribute.Int64("n", 2)), `{"msg":"hello","n":2}`},
		{"slice", attribute.SliceValue(attribute.StringValue("a"), attribute.BoolValue(false)), `["a",false]`},
		{"strings", attribute.StringSliceValue([]string{"a", "b"}), `["a","b"]`},
		{"nan", attribute.Float64Value(math.NaN()), "NaN"},
	} {
		if got := bodyString(c.in); got != c.want {
			t.Errorf("%s: bodyString = %q, want %q", c.name, got, c.want)
		}
	}
}
