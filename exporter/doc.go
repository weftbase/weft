// Package exporter is the local developer platform's side of weft's
// observability: the destinations that write into Weft Studio's store
// and the zero-config behaviour a developer expects on top of weft/otel.
//
//	defer exporter.Install()() // a running `weft studio`, else ./.weft/weft.db
//
//	defer exporter.Install(
//	    exporter.Local("weft.db"),                        // obsdb/sqlite, synchronous, content on
//	    exporter.Studio("https://studio.example", token), // OTLP with a bearer token, content on
//	    otel.Datadog(),                                   // any weft/otel option beside them
//	)()
//
// weft/otel knows nothing of Studio: it emits, and reads only the
// standard OpenTelemetry variables. This package builds on its public
// seams — otel.Sink (Local), otel.OTLP with otel.FirstParty (Studio),
// otel.Fallback (Install's default) — and adds what the platform reads:
//
//	WEFT_STUDIO_URL, WEFT_STUDIO_TOKEN → a Studio destination (content on)
//	(neither, nor WEFT_DISCOVERY=off)   → the discovery file a running `weft studio` / `weft dev` wrote
//	WEFT_DB                             → the default Local path (else ./.weft/weft.db)
//
// The discovery file is read only by Install and Start, when their
// options name no Local and no Studio, without NoDiscovery or
// otel.NoEnv. Install falls back to Local("") when nothing else is
// configured.
//
// The package belongs to the platform (it imports obsdb); framework
// packages never import it. It moves to the weftbase repository with
// studio, obsdb and runtime.
package exporter
