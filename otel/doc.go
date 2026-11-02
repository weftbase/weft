// Package otel wires weft's observability to one or more
// OpenTelemetry destinations at once — any OTLP endpoint, Datadog,
// Langfuse, your own exporters or a synchronous Sink — each with its
// own signals and content policy (ADR 0024, S2).
//
// One Install, one set of spans and records, fanned out:
//
//	defer otel.Install(
//	    otel.OTLP("https://collector.internal:4318", otel.FirstParty()), // your own collector: content on
//	    otel.Datadog(),                                 // the local Datadog Agent's OTLP intake, content off
//	    otel.Langfuse("https://cloud.langfuse.com", pk, sk), // traces only
//	)()
//
// The package reads the standard OpenTelemetry variables only
// (OTEL_EXPORTER_OTLP_ENDPOINT and _HEADERS, OTEL_SERVICE_NAME,
// OTEL_RESOURCE_ATTRIBUTES, the GenAI content-capture variable) plus
// WEFT_ENV for the development gate. With nothing configured Install
// records nothing and says so in one INFO line. A local viewer's store
// plugs in as a Sink (a synchronous, first-party destination) or as an
// OTLP destination marked FirstParty; weft itself knows no viewer.
//
// The rules that shape it:
//
//   - Content is captured once and shaped per destination [D2]. Every
//     processor registered here answers the Logs API's Enabled by event
//     name: the core asks of each pure-content kind ("weft.messages",
//     and ADR 0028's "weft.prompt" and "weft.tools") and emits content
//     when any destination wants it. Content-off chains clone
//     the record first (the SDK hands every processor the same pointer),
//     strip the body with core.StripContent, empty a request record's
//     params.stop and drop messages, prompt and tools records;
//     content-on chains redact and cap event and delta bodies and
//     redact (never cap) the transcript records part by part. The core reads no environment variable — this
//     module does, here only.
//   - The run tracker keeps the set of open runs and emits one
//     weft.heartbeat record per open run per interval, so a 45 s tool
//     call reads running and a crashed process stops heartbeating (the
//     "interrupted" rule stays honest). Sinks never store heartbeats.
//   - The slog bridge (Slog wraps slog.Default; SlogHandler wraps any
//     handler) tees every line logged on a span-carrying context to the
//     pipeline as an app log record with that span's ids — weft.Logger's
//     run lines, mw.Log's, the app's own — for the destinations AppLogs
//     admits (first-party destinations with content on by default), each line
//     shaped by the destination's content policy (Redact, MaxBytes;
//     stripped when content is off). While it is live, heartbeats and
//     each run's invoke_agent span carry weft.logs.bridged on the
//     destinations that take the lines; a viewer reads its absence as
//     "no weft slog bridge was recorded for this run".
//   - A Sink is written synchronously: nothing emitted is lost before
//     Emit returns.
//   - Install never panics and never fails the program; Start returns
//     errors and leaves the globals alone under NoGlobal (tests).
//
// This module depends on the OTel SDK and the OTLP/HTTP exporters; the
// core never does. It never imports weft/thread, and nothing of a
// platform (a store, a viewer): those import otel, never the reverse.
package otel
