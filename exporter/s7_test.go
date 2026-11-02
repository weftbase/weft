package exporter

// The S7 gates that read the store back: the Local sink (obsdb/sqlite
// behind otel.Sink) holds the content, every durable record, the
// heartbeats' last-seen and the live hub — moved here from weft/otel,
// which pins the same pipeline behaviour against an in-memory Sink.

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/weftgo/weft/core"
	"github.com/weftgo/weft/obsdb"
	"github.com/weftgo/weft/otel"
	"github.com/weftgo/weft/wefttest"

	// The kill-9 and heartbeat-row tests read the SQLite file directly.
	_ "modernc.org/sqlite"
)

// S7 gate 1: Local + Exporters(inMemory) + a stripped destination at
// once — Local has content, the stripped one has none, both have every
// durable record (the clone rule: no chain sees another's edits).
func TestS7LocalPlusMemoryPlusStripped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s7.db")
	stripped := newMemExporter()
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(),
		Local(path),
		otel.Exporters(nil, stripped, otel.NoContent()),
	)
	if err != nil {
		t.Fatal(err)
	}
	res, runID := agentThrough(t, p)
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}

	// Local has content: the transcript is there and rebuilds the run's
	// messages byte-for-byte (the replay gate, exercised fully below).
	db, err := sqliteOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tr, err := db.Transcript(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tr) == 0 {
		t.Fatal("Local stored no messages records — its content default is on")
	}
	rebuilt := []byte{'['}
	for i, batch := range tr {
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
		t.Fatalf("Local transcript mismatch:\n got %s\nwant %s", rebuilt, want)
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

	// And Local saw every durable record too.
	page, err := db.Events(context.Background(), runID, -1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 8 {
		t.Errorf("Local saw %d event records, want 8", len(page.Events))
	}
}

// S7 gate 3: heartbeats keep a long quiet tool call reading running and
// store no rows. The real numbers are a 10 s interval under a 45 s tool
// call; the test scales them (30 ms under a blocked call).
func TestS7HeartbeatKeepsLongToolRunning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hb.db")
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(),
		Local(path),
		otel.Heartbeat(30*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	slow := core.Tool("slow", "Blocks until released.", func(ctx context.Context, _ struct{}) (string, error) {
		close(started)
		<-release
		return "done", nil
	})
	agt := core.New(
		wefttest.Script(
			wefttest.ToolCalls(wefttest.Call{Name: "slow"}),
			wefttest.Say("done"),
		),
		core.Name("hb"),
		core.TracerProvider(p.TracerProvider()),
		core.LoggerProvider(p.LoggerProvider()),
		slow,
	)
	done := make(chan struct{})
	go func() {
		_, _ = agt.Generate(context.Background(), core.Prompt("go"),
			core.Metadata(map[string]string{"weft.session.id": "s7-hb"}))
		close(done)
	}()
	<-started

	db := LocalDBOf(p)
	var first time.Time
	deadline := time.Now().Add(3 * time.Second)
	for first.IsZero() && time.Now().Before(deadline) {
		page, err := db.Runs(context.Background(), obsdb.RunQuery{})
		if err == nil && page.Total > 0 && page.Runs[0].Status == obsdb.StatusRunning {
			first = page.Runs[0].LastSeen
		} else {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if first.IsZero() {
		close(release)
		<-done
		t.Fatal("the run never read running mid-tool-call")
	}
	// The quiet window: nothing but heartbeats can move last-seen.
	time.Sleep(90 * time.Millisecond)
	page, err := db.Runs(context.Background(), obsdb.RunQuery{})
	if err != nil || page.Total != 1 {
		close(release)
		<-done
		t.Fatalf("mid-run read: %v", err)
	}
	if page.Runs[0].Status != obsdb.StatusRunning {
		close(release)
		<-done
		t.Fatalf("quiet run read %q, want running (heartbeats are fresh evidence)", page.Runs[0].Status)
	}
	if !page.Runs[0].LastSeen.After(first) {
		close(release)
		<-done
		t.Fatal("last-seen did not advance during the quiet tool call — no heartbeats flowed")
	}
	close(release)
	<-done
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}

	// No heartbeat rows were stored.
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(30000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	var hb int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM records WHERE kind='heartbeat'`).Scan(&hb); err != nil {
		t.Fatal(err)
	}
	if hb != 0 {
		t.Errorf("%d heartbeat rows stored; heartbeats only move last-seen", hb)
	}
}

// S7 gate 4: kill -9 mid-run. The child writes through a Local pipeline
// into a real file; SIGKILL leaves the run with no terminal record; the
// parent reads it running while last-seen is fresh, and interrupted
// once it is older than InterruptedAfter (simulated by ageing
// last_seen_ns — the sole input of the derivation — rather than
// sleeping 30 s; the boundary itself is pinned by obsdb's tests).
func TestS7Kill9ShowsInterrupted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	src := filepath.Join(dir, "child.go")
	child := "package main\n\nimport (\n" +
		"\t\"context\"\n\t\"os\"\n\t\"time\"\n\n" +
		"\tweftotel \"github.com/weftgo/weft/otel\"\n\t\"github.com/weftgo/weft/exporter\"\n\n" +
		"\t\"github.com/weftgo/weft\"\n\t\"github.com/weftgo/weft/wefttest\"\n)\n\n" +
		"func main() {\n" +
		"\tshutdown := exporter.Install(\n" +
		"\t\texporter.Local(" + "`" + dbPath + "`" + "),\n" +
		"\t\tweftotel.Heartbeat(50 * time.Millisecond),\n" +
		"\t)\n\tdefer shutdown()\n" +
		"\tslow := weft.Tool(\"slow\", \"Outlives the process.\", func(ctx context.Context, _ struct{}) (string, error) {\n" +
		"\t\ttime.Sleep(10 * time.Minute)\n\t\treturn \"never\", nil\n\t})\n" +
		"\tagt := weft.New(\n" +
		"\t\twefttest.Script(wefttest.ToolCalls(wefttest.Call{Name: \"slow\"}), wefttest.Say(\"late\")),\n" +
		"\t\tweft.Name(\"victim\"),\n" +
		"\t\tslow,\n\t)\n" +
		"\t_, _ = agt.Generate(context.Background(), weft.Prompt(\"go\"),\n" +
		"\t\tweft.Metadata(map[string]string{\"weft.session.id\": \"crash\"}))\n" +
		"\tos.Exit(0)\n}\n"
	if err := os.WriteFile(src, []byte(child), 0o644); err != nil {
		t.Fatal(err)
	}
	// Build the child to a binary and run THAT: killing a "go run"
	// wrapper orphans the compiled child, which would keep heartbeating
	// through the "crash" (go run execs the binary as its own process).
	// The build runs from this module's directory, where the workspace
	// resolves weft, obsdb, otel and exporter locally.
	bin := filepath.Join(dir, "child.bin")
	build := exec.Command("go", "build", "-o", bin, src)
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the child: %v: %s", err, out)
	}
	cmd := exec.Command(bin)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "WEFT_MODEL_REQUESTS=deny")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Wait until the run exists, then kill -9.
	deadline := time.Now().Add(60 * time.Second)
	for {
		page, err := readRuns(dbPath)
		if err == nil && len(page) > 0 && page[0].Status == obsdb.StatusRunning {
			break
		}
		if !time.Now().Before(deadline) {
			_ = cmd.Process.Kill()
			t.Fatal("the child's run never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil { // SIGKILL: no flush, no run_finish
		t.Fatal(err)
	}
	_ = cmd.Wait()

	// Fresh crash: running (evidence, not a corpse).
	page, err := readRuns(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Status != obsdb.StatusRunning {
		t.Fatalf("after kill -9: %+v", page)
	}
	// Age the last-seen past InterruptedAfter — the only input the rule
	// reads — and read again: interrupted.
	raw, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(30000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE runs SET last_seen_ns = last_seen_ns - ?`,
		(obsdb.InterruptedAfter + time.Second).Nanoseconds()); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	page, err = readRuns(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Status != obsdb.StatusInterrupted {
		t.Fatalf("after the crash window: %+v, want interrupted", page)
	}
}

func readRuns(path string) ([]obsdb.RunRow, error) {
	db, err := sqliteOpen(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	page, err := db.Runs(context.Background(), obsdb.RunQuery{})
	if err != nil {
		return nil, err
	}
	return page.Runs, nil
}

// S7 gate 5: a hub subscriber sees a record before Write returns —
// through the Local destination's shared handle.
func TestS7HubSeesRecordBeforeWriteReturns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(), Local(path))
	if err != nil {
		t.Fatal(err)
	}
	db := LocalDBOf(p)
	hub := db.(interface{ Hub() obsdb.Hub }).Hub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames, err := hub.Subscribe(ctx, obsdb.Selector{Agent: "s7"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	sawRecord := make(chan obsdb.Frame, 1)
	go func() {
		for f := range frames {
			if f.Kind == obsdb.FrameRecord {
				sawRecord <- f
				return
			}
		}
	}()
	written := make(chan struct{})
	go func() {
		_, _ = agentThrough(t, p)
		close(written)
	}()
	select {
	case f := <-sawRecord:
		if f.Record == nil || f.Seq == 0 {
			t.Fatalf("frame = %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no record frame before the run's Write returned")
	}
	<-written
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}

// The store's replay test, ported: a run recorded through Local
// rebuilds the transcript byte-for-byte from DB.Transcript.
func TestS7ReplayRebuildsTranscriptByteForByte(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.db")
	p, err := Start(testCtx(t), otel.NoGlobal(), otel.NoEnv(), Local(path))
	if err != nil {
		t.Fatal(err)
	}
	res, runID := agentThrough(t, p)
	if err := p.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	db, err := sqliteOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tr, err := db.Transcript(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	// The batches concatenate to res.Messages exactly.
	rebuilt := []byte{'['}
	for i, batch := range tr {
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
		t.Fatalf("transcript mismatch:\n got %s\nwant %s", rebuilt, want)
	}
	// And the run reads succeeded with the identity chain intact.
	det, err := db.Run(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if det.Status != obsdb.StatusSucceeded {
		t.Errorf("status = %q, want succeeded", det.Status)
	}
	if det.Agent != "s7" || det.SessionID != "s7-session" || det.Meta["tenant"] != "acme" {
		t.Errorf("identity = %+v meta=%v", det.RunRow, det.Meta)
	}
}

// Install's zero-config rule: with no options and no env it writes the
// local sink only (WEFT_DB pointed at a temp file), and the returned
// function shuts down cleanly.
func TestInstallZeroConfigLocalOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zero.db")
	t.Setenv("WEFT_DB", path)
	isolate(t)
	shutdown := Install()
	p := otel.Installed()
	if p == nil || LocalDBOf(p) == nil || LocalDB() == nil {
		shutdown()
		t.Fatal("zero-config Install did not open the local sink")
	}
	// One scripted run through the installed globals.
	agt := core.New(
		wefttest.Script(wefttest.Say("zero")),
		core.Name("zero"),
	)
	if _, err := agt.Generate(context.Background(), core.Prompt("go"),
		core.Metadata(map[string]string{"weft.session.id": "zero"})); err != nil {
		t.Fatal(err)
	}
	shutdown()
	// The run landed in the sink with content on (Local's default).
	db, err := sqliteOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	page, err := db.Runs(context.Background(), obsdb.RunQuery{})
	if err != nil || page.Total != 1 {
		t.Fatalf("runs after Install(): %d, %v", page.Total, err)
	}
	tr, err := db.Transcript(context.Background(), page.Runs[0].ID)
	if err != nil || len(tr) == 0 {
		t.Fatalf("transcript = %v, %v: content must be on", tr, err)
	}
}
