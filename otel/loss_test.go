package otel

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// stuckSinkLogs never finishes an export until its ctx ends — or,
// with ignoreCtx, never at all (until release closes).
type stuckSinkLogs struct {
	ignoreCtx bool
	release   chan struct{}
}

func (h stuckSinkLogs) Export(ctx context.Context, _ []sdklog.Record) error {
	if h.ignoreCtx {
		<-h.release
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}
func (stuckSinkLogs) Shutdown(context.Context) error   { return nil }
func (stuckSinkLogs) ForceFlush(context.Context) error { return nil }

// E3 (R8, R12): a hung Sink never stalls the run whose records it
// writes. Each synchronous export is bounded by the destination's
// Timeout (default 2 s); past it the export is abandoned and counted
// dropped, and while it is stuck every later record is dropped at once
// — so a run of many records finishes in about one Timeout, whether or
// not the exporter honours its ctx.
func TestHungSinkDoesNotStallTheRun(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		release := make(chan struct{})
		p, err := Start(context.Background(), NoGlobal(), NoEnv(), Sink("store", func(context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error) {
			return nil, stuckSinkLogs{ignoreCtx: ignore, release: release}, nil
		}, Timeout(100*time.Millisecond)))
		if err != nil {
			t.Fatal(err)
		}
		agt := core.New(wefttest.Script(wefttest.Say("hi")), core.Name("a"),
			core.TracerProvider(p.TracerProvider()), core.LoggerProvider(p.LoggerProvider()))
		start := time.Now()
		done := make(chan error, 1)
		go func() { _, err := agt.Generate(context.Background(), core.Prompt("go")); done <- err }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("ignoreCtx=%v: the run is blocked on a hung Sink", ignore)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("ignoreCtx=%v: the run took %v behind a Sink with a 100ms Timeout", ignore, d)
		}
		if n := p.dests[0].logProc.(*destProc).drops.count.Load(); n == 0 {
			t.Errorf("ignoreCtx=%v: no record counted dropped", ignore)
		}
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = p.Shutdown(ctx)
		cancel()
	}
}

// The default bound, with no Timeout option: about two seconds.
func TestHungSinkDefaultBound(t *testing.T) {
	if testing.Short() {
		t.Skip("waits the 2 s default")
	}
	g := newSinkGate(0, newDropCounter("t"))
	if g.timeout != defaultSinkTimeout || defaultSinkTimeout != 2*time.Second {
		t.Fatalf("default %v", g.timeout)
	}
	start := time.Now()
	g.export(context.Background(), 1, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	if d := time.Since(start); d < defaultSinkTimeout || d > defaultSinkTimeout+time.Second {
		t.Errorf("export bounded after %v", d)
	}
	if g.drops.count.Load() != 1 {
		t.Errorf("dropped %d", g.drops.count.Load())
	}
}

// hangBatchLogs is a batch destination's exporter that never finishes
// an export until its ctx ends.
type hangBatchLogs struct{}

func (hangBatchLogs) Export(ctx context.Context, _ []sdklog.Record) error {
	<-ctx.Done()
	return ctx.Err()
}
func (hangBatchLogs) Shutdown(context.Context) error   { return nil }
func (hangBatchLogs) ForceFlush(context.Context) error { return nil }

// E4 (WR-36): records a hung destination still holds at Shutdown are
// lost, and Shutdown says so — a *LossError matching ErrRecordsLost,
// counted per destination — within the caller's ctx: the flush takes
// half of it, the providers' shutdown the rest.
func TestShutdownReportsLoss(t *testing.T) {
	p, err := Start(context.Background(), NoGlobal(), NoEnv(), Heartbeat(0), Exporters(nil, hangBatchLogs{}))
	if err != nil {
		t.Fatal(err)
	}
	agentThrough(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = p.Shutdown(ctx)
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Errorf("Shutdown took %v with a 1.5 s ctx", d)
	}
	if !errors.Is(err, ErrRecordsLost) {
		t.Fatalf("Shutdown = %v, want ErrRecordsLost", err)
	}
	var loss *LossError
	if !errors.As(err, &loss) || loss.Lost == 0 || loss.ByDestination["exporters"] != loss.Lost {
		t.Fatalf("loss = %+v", loss)
	}
	if err2 := p.Shutdown(context.Background()); !errors.Is(err2, ErrRecordsLost) {
		t.Errorf("a second Shutdown = %v, want the first's result", err2)
	}
}

// A pipeline that lost nothing shuts down clean.
func TestShutdownCleanWithoutLoss(t *testing.T) {
	mem := newMemExporter()
	p, err := Start(context.Background(), NoGlobal(), NoEnv(), Heartbeat(0), Exporters(nil, mem))
	if err != nil {
		t.Fatal(err)
	}
	agentThrough(t, p)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
}

func TestFlushBudget(t *testing.T) {
	if got := flushBudget(context.Background()); got != 5*time.Second {
		t.Errorf("no deadline: %v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if got := flushBudget(ctx); got > 2*time.Second || got < 1900*time.Millisecond {
		t.Errorf("4 s ctx: flush budget %v, want half", got)
	}
}

// overlapLogs is a Sink log exporter that blocks every export, ignoring
// its ctx, until release closes, and notes whether its Shutdown ever
// ran while an Export was still inside the exporter.
type overlapLogs struct {
	release  chan struct{}
	inExport *atomic.Int32
	shutdown *atomic.Int32
	overlap  *atomic.Bool
}

func (h overlapLogs) Export(context.Context, []sdklog.Record) error {
	h.inExport.Add(1)
	defer h.inExport.Add(-1)
	<-h.release
	return nil
}

func (h overlapLogs) Shutdown(context.Context) error {
	h.shutdown.Add(1)
	if h.inExport.Load() > 0 {
		h.overlap.Store(true)
	}
	return nil
}
func (overlapLogs) ForceFlush(context.Context) error { return nil }

// The pipeline's Shutdown never shuts a Sink's exporters down under an
// export the gate abandoned and that is still running: it waits for it
// (bounded by its ctx and one more Timeout) and, when the export returns
// in time, shuts down as usual; when it does not, it leaves the
// exporters open and says so in its error — never the concurrency an
// exporter never had.
func TestSinkShutdownNeverOverlapsAnExport(t *testing.T) {
	for _, returns := range []bool{true, false} {
		h := overlapLogs{release: make(chan struct{}), inExport: new(atomic.Int32), shutdown: new(atomic.Int32), overlap: new(atomic.Bool)}
		p, err := Start(context.Background(), NoGlobal(), NoEnv(), Heartbeat(0), Sink("store", func(context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error) {
			return nil, h, nil
		}, Timeout(50*time.Millisecond)))
		if err != nil {
			t.Fatal(err)
		}
		agentThrough(t, p) // the first export is abandoned and stays inside Export
		if h.inExport.Load() == 0 {
			t.Fatal("no export left running")
		}
		if returns {
			go func() { time.Sleep(20 * time.Millisecond); close(h.release) }()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err = p.Shutdown(ctx)
		cancel()
		if h.overlap.Load() {
			t.Errorf("returns=%v: Shutdown ran while an export was still inside the exporter", returns)
		}
		if returns {
			if h.shutdown.Load() != 1 {
				t.Errorf("returns=%v: the exporter was shut down %d times, want once", returns, h.shutdown.Load())
			}
		} else {
			if h.shutdown.Load() != 0 {
				t.Errorf("returns=%v: the exporter was shut down under a running export", returns)
			}
			if err == nil || !strings.Contains(err.Error(), "still running at shutdown") {
				t.Errorf("returns=%v: Shutdown = %v, want it to say the export was still running", returns, err)
			}
			close(h.release)
		}
	}
}

// A run canceled mid-tool emits its tail records on its canceled ctx;
// the Sink's export deadline is the gate's alone, so a Sink that writes
// at once writes them — none counted lost, Shutdown clean.
func TestCanceledRunTailRecordsReachTheSink(t *testing.T) {
	mem := newMemExporter()
	p, err := Start(context.Background(), NoGlobal(), NoEnv(), Heartbeat(0), Sink("store", func(context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error) {
		return nil, mem, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	hang := core.Tool("hang", "Hang.", func(ctx context.Context, _ struct{}) (string, error) {
		cancel()
		<-ctx.Done()
		return "", ctx.Err()
	})
	agt := core.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "hang", ID: "c1", Args: `{}`}),
		wefttest.Say("x"),
	), core.Name("a"), hang, core.TracerProvider(p.TracerProvider()), core.LoggerProvider(p.LoggerProvider()))
	_, _ = agt.Generate(ctx, core.Prompt("go"))
	if lost := p.dests[0].drops.lost(); lost != 0 {
		t.Errorf("%d records counted lost after a canceled run (written %d)", lost, len(mem.snapshot()))
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown = %v", err)
	}
}

// A working Sink and a normal run shut down with no error.
func TestCleanSinkShutdown(t *testing.T) {
	mem := newMemExporter()
	p, err := Start(context.Background(), NoGlobal(), NoEnv(), Sink("store", func(context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error) {
		return nil, mem, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	agentThrough(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("clean Shutdown = %v", err)
	}
}

// slowBatchLogs exports slowly and never fails.
type slowBatchLogs struct {
	delay    time.Duration
	exported atomic.Int64
}

func (e *slowBatchLogs) Export(_ context.Context, recs []sdklog.Record) error {
	time.Sleep(e.delay)
	e.exported.Add(int64(len(recs)))
	return nil
}
func (*slowBatchLogs) Shutdown(context.Context) error   { return nil }
func (*slowBatchLogs) ForceFlush(context.Context) error { return nil }

// Records a slow (not failing) destination still has queued when
// Shutdown's context ends are lost like any other: counted, and
// reported as ErrRecordsLost. The flush is cut between chunks when the
// chain closes; the chunks it never began used to be neither exported
// nor counted, and Shutdown named only its deadline.
func TestShutdownCountsRecordsASlowDestinationNeverExported(t *testing.T) {
	exp := &slowBatchLogs{delay: 150 * time.Millisecond}
	p, err := Start(context.Background(), NoEnv(), NoGlobal(), Heartbeat(0),
		Exporters(nil, exp, BatchDelay(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	l := p.LoggerProvider().Logger("slow")
	const emitted = 1700 // four export chunks of the SDK's 512
	for range emitted {
		var r otellog.Record
		r.SetEventName("slow")
		l.Emit(context.Background(), r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = p.Shutdown(ctx)
	if !errors.Is(err, ErrRecordsLost) {
		t.Fatalf("Shutdown = %v, want ErrRecordsLost: the queue was not empty when the context ended", err)
	}
	var loss *LossError
	if !errors.As(err, &loss) {
		t.Fatalf("Shutdown = %v, want a *LossError", err)
	}
	time.Sleep(2 * exp.delay) // an export still running when Shutdown returned finishes
	if got := exp.exported.Load(); got+loss.Lost < emitted {
		t.Fatalf("%d emitted, %d exported, %d reported lost: %d records neither exported nor counted",
			emitted, got, loss.Lost, emitted-got-loss.Lost)
	}
}
