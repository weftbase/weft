package exporter_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/weftgo/weft"
	"github.com/weftgo/weft/exporter"
	"github.com/weftgo/weft/mw"
	"github.com/weftgo/weft/obsdb"
	"github.com/weftgo/weft/obsdb/sqlite"
	"github.com/weftgo/weft/otel"
	"github.com/weftgo/weft/wefttest"
)

// Local is the local sink: every record written into obsdb/sqlite
// before Emit returns, content on. otel.NoEnv keeps the example
// hermetic — no WEFT_* / OTEL_* variable (and no discovery file) can add
// a destination behind it.
func ExampleLocal() {
	dir, err := os.MkdirTemp("", "weft-otel-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	shutdown := exporter.Install(
		otel.NoEnv(),
		exporter.Local(filepath.Join(dir, "weft.db")),
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
	shutdown() // flush every destination, close the local DB last

	// The sink reopens as a plain obsdb.DB: the run row with its
	// derived status, and the transcript the messages records hold.
	db, err := sqlite.Open(filepath.Join(dir, "weft.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	page, err := db.Runs(context.Background(), obsdb.RunQuery{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(page.Runs[0].Status, page.Runs[0].MessageCount)
	// Output: succeeded 2
}

// With otel.Slog the app's log lines reach the Local sink under the span
// they were logged in, and read back as the run's app logs — what
// Studio's runs/{id}/logs serves.
func ExampleLocal_appLogs() {
	dir, err := os.MkdirTemp("", "weft-otel-slog-")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "weft.db")
	shutdown := exporter.Install(otel.NoEnv(), exporter.Local(path), otel.Slog())

	agt := weft.New(wefttest.Script(wefttest.Say("all done")),
		weft.Name("example"), weft.WrapModel(mw.Log(nil)))
	res, err := agt.Generate(context.Background(), weft.Prompt("go"))
	if err != nil {
		log.Fatal(err)
	}
	shutdown() // flushes, and puts slog.Default back

	db, err := sqlite.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	page, err := db.OtherLogs(context.Background(), res.ID, obsdb.LogQuery{})
	if err != nil {
		log.Fatal(err)
	}
	for _, l := range page.Logs {
		fmt.Println(obsdb.SeverityText(l.Severity), l.Body)
	}
	// Output:
	// DEBUG run start
	// DEBUG model request
	// DEBUG model finish
	// DEBUG model call
	// DEBUG run finish
}
