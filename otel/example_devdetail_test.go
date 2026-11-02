package otel_test

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/weftbase/weft"
	"github.com/weftbase/weft/mw"
	"github.com/weftbase/weft/otel"
	"github.com/weftbase/weft/wefttest"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// DevDetail records the whole middleware onion in development: one
// "layer <name>" span per WrapModel / WrapTools layer per call, nested
// under the call's own span, for the first-party destinations (here the
// app's own exporter, marked FirstParty).
func ExampleDevDetail() {
	spans := tracetest.NewInMemoryExporter()
	p, err := otel.Start(context.Background(),
		otel.NoEnv(), otel.DevMode(), otel.NoGlobal(),
		otel.Exporters(spans, nil, otel.FirstParty()),
		otel.DevDetail(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()

	lookup := weft.Tool("lookup", "", func(context.Context, struct{}) (string, error) { return "found", nil })
	agt := weft.New(wefttest.Script(
		wefttest.ToolCalls(wefttest.Call{Name: "lookup"}),
		wefttest.Say("done")),
		lookup, weft.WrapTools(mw.Audit(nil), mw.Allow(nil)),
		weft.TracerProvider(p.TracerProvider()), weft.LoggerProvider(p.LoggerProvider()))
	if _, err := agt.Generate(context.Background(), weft.Prompt("look it up")); err != nil {
		log.Fatal(err)
	}
	_ = p.ForceFlush(context.Background())
	for _, s := range spans.GetSpans() {
		if !strings.HasPrefix(s.Name, "layer ") {
			continue
		}
		for _, kv := range s.Attributes {
			if kv.Key == "weft.layer.index" {
				fmt.Println(s.Name, kv.Value.Emit())
			}
		}
	}
	// Unordered output:
	// layer mw.Audit 0
	// layer mw.Allow 1
}
