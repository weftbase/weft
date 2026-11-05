package otel

import (
	"context"
	"time"

	"github.com/weftbase/weft/core"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Option configures the pipeline (Install/Start).
type Option interface{ apply(*config) }

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

// flagOption is an option without parameters, a comparable value: a
// package wrapping otel can recognise it among the options it is handed
// (opt == otel.NoEnv()).
type flagOption int

const (
	flagNoEnv flagOption = iota + 1
	flagNoGlobal
)

func (f flagOption) apply(c *config) {
	switch f {
	case flagNoEnv:
		c.noEnv = true
	case flagNoGlobal:
		c.noGlobal = true
	}
}

// Options bundles opts into one Option — for a package that offers a
// preset made of several of otel's own options. The result is a
// pointer, so it is comparable: a wrapper may recognise the presets it
// handed out among the options it is given.
func Options(opts ...Option) Option { return &optionSet{opts: append([]Option(nil), opts...)} }

type optionSet struct{ opts []Option }

func (o *optionSet) apply(c *config) {
	for _, x := range o.opts {
		x.apply(c)
	}
}

// Fallback names destinations used only when the pipeline would
// otherwise have none: no destination option and no environment
// destination (OTEL_EXPORTER_OTLP_ENDPOINT, or one Environment marks)
// — and, under Install, when
// only environment destinations were configured and every one failed to
// build (one WARN). A program that names its own destinations never
// gets a fallback one. Only the destinations among opts count; any
// other option inside it is ignored.
func Fallback(opts ...Option) Option {
	return optionFunc(func(c *config) {
		var fb config
		for _, o := range opts {
			o.apply(&fb)
		}
		c.fallback = append(c.fallback, fb.dests...)
	})
}

// ContentConfig carries the caps and redaction that live per
// destination, not in the core ([D2]: the core cannot discover them —
// the OTel global never hands out the SDK provider).
type ContentConfig struct {
	// MaxBytes caps one event field or delta body, a prompt record's
	// text and a tools record's body (whole tool entries dropped from
	// the end, so the body stays JSON); 0 = 32 KiB; negative =
	// unlimited. A cut sets weft.content.truncated_bytes. The default
	// applies to tools bodies too: a large catalog (an MCP server's, say)
	// arrives truncated, with truncated_bytes set — raise MaxBytes for
	// the local sink when the full catalog is wanted. Never applied to
	// messages records — a capped transcript is not replay-grade.
	MaxBytes int
	// Redact is applied to a content field before the cap; nil = identity.
	// It sees event and delta bodies and weft.messages transcript
	// records, the latter part by part with the kind the same content
	// has on the event path: text parts (every role) core.ContentText,
	// reasoning core.ContentReasoning, tool-call args core.ContentArgs (an
	// output that is not JSON is carried as a JSON string), tool results
	// core.ContentResult. It also sees a prompt record's text
	// (core.ContentPrompt) and each stop sequence of a request record
	// (core.ContentStop); a tools record is capped, never redacted (ADR
	// 0028 §6), and a bridged app log line's message and string
	// attributes (ContentLog; Slog, SlogHandler — capped too, a panic
	// drops the line). Ids, names, roles, signatures, file parts and hashes are
	// not passed to it; a transcript record is redacted, never capped. It
	// runs on the run's goroutine: a panic in it is contained — the event
	// goes out stripped, the transcript record not at all — and counted
	// as the destination's drop; so is a transcript record that cannot be
	// decoded for redaction. Nothing it was given is ever sent unredacted.
	Redact func(kind core.ContentKind, s string) string
}

// Content sets the pipeline-wide default for destinations with content
// on; a destination's WithContent overrides it.
func Content(cfg ContentConfig) Option {
	return optionFunc(func(c *config) { c.content = cfg })
}

// Service sets resource service.name. Else OTEL_SERVICE_NAME, else the
// binary's name.
func Service(name string) Option {
	return optionFunc(func(c *config) { c.service = name })
}

// Resource merges r over the detected resource.
func Resource(r *sdkresource.Resource) Option {
	return optionFunc(func(c *config) {
		if r != nil {
			c.resource = r
		}
	})
}

// Heartbeat sets how often the run tracker emits one weft.heartbeat
// record per open run (the default 10s; 0 or less disables). Three missed
// intervals are what obsdb's InterruptedAfter reads as interrupted.
func Heartbeat(every time.Duration) Option {
	return optionFunc(func(c *config) { c.heartbeat = every })
}

// Sampler sets the trace sampler (default ParentBased(AlwaysOn)).
// Records are log records; sampling never applies to them.
func Sampler(s sdktrace.Sampler) Option {
	return optionFunc(func(c *config) {
		if s != nil {
			c.sampler = s
		}
	})
}

// NoEnv ignores the OTEL_* destination variables (envDestinations).
// Explicit options are unaffected. It reads nothing, WEFT_ENV included:
// under NoEnv the development gate RawBodies and Blocking need holds
// only with DevMode (Start's WARN says "otel.NoEnv reads no WEFT_ENV"
// otherwise). The resource still reads OTEL_SERVICE_NAME and
// OTEL_RESOURCE_ATTRIBUTES (the SDK's own detector). The value is
// comparable (opt == otel.NoEnv()), for a wrapper that reads variables
// of its own.
func NoEnv() Option { return flagNoEnv }

// DevMode attests in code that this pipeline runs in development: it
// satisfies the dev gate (ADR 0028 §12) that RawBodies, DevDetail and a
// destination's Blocking need, without reading the environment. It is
// the code-side twin of WEFT_ENV=dev, for tests and harnesses that
// control their own process — typically beside NoEnv, which reads no
// WEFT_ENV and so refuses all three on its own. It unlocks those three
// and nothing else: no destination follows from it. The
// gate still applies — a production binary never sets DevMode; the
// attestation has only moved from the process environment into code.
// The operator's environment wins: when the pipeline reads it (no
// NoEnv) and WEFT_ENV is set to anything but "dev", DevMode is refused
// with one WARN ("otel: DevMode ignored: WEFT_ENV=<value>") and the
// switches stay off; it opens the gate under NoEnv or with WEFT_ENV
// unset. Whenever DevMode is what opened it, Start logs one INFO line
// per switch it unlocked ("otel: raw bodies on by otel.DevMode()",
// "otel: dev detail on by otel.DevDetail() under otel.DevMode()",
// "otel: Blocking on by otel.DevMode()"), so a binary that ships it is
// visible.
func DevMode() Option {
	return optionFunc(func(c *config) { c.devMode = true })
}

// RawBodies records each model call attempt's wire exchange — the
// provider request and response the first-party adapters sent and
// received, headers included minus the credential ones, each body
// capped at 256 KiB — as one weft.raw record per attempt (ADR 0028
// §12), for first-party destinations only (a Sink, or one marked
// FirstParty; never Datadog, Langfuse, or an OTLP or Exporters
// destination without FirstParty, whatever its content setting, and not
// a first-party destination under NoContent). Off by default, and
// refused outside development: it takes effect only when the pipeline
// starts under the dev gate — WEFT_ENV is "dev" (read unless NoEnv), or
// DevMode attests it in code (under NoEnv, or with WEFT_ENV unset; an
// explicit WEFT_ENV other than dev wins, "otel: DevMode ignored:
// WEFT_ENV=<value>"). Otherwise Start logs one WARN line naming
// the cause ("otel: RawBodies ignored: WEFT_ENV is not dev", or under
// NoEnv without DevMode "otel: RawBodies ignored: otel.NoEnv reads no
// WEFT_ENV") and the pipeline behaves as if the option were absent: no destination asks for weft.raw, so the
// adapters buffer nothing (core.Reporter.WantsRaw is false). The gate
// is the pipeline's LoggerProvider marker (LoggerProvider.RawBodies):
// pass Pipeline.LoggerProvider() to weft.LoggerProvider, or rely on
// Install's global registration — a stock SDK provider never records
// raw bodies. Under it a first-party batch destination's log batch is
// capped at 16 records and each export is split at 12 MiB, keeping
// every post under a 16 MiB ingest limit: a raw record is ≈ 650 KiB with two full
// ordinary bodies and at worst ≈ 3 MiB (bodies of control bytes, each
// six bytes in JSON; it is encoded without HTML escaping).
func RawBodies(on bool) Option {
	return optionFunc(func(c *config) { c.rawBodies = on })
}

// DevDetail turns on the development detail level (ADR 0016's
// 2026-10-26 amendment): beside RawBodies, the record shows the whole
// middleware onion. Each model call and tool call gets one "layer
// <name>" span per WrapModel / WrapTools layer (weft.layer.name,
// weft.layer.kind, weft.layer.index; error.type where the layer
// returned an error), nested outer to inner under its chat or
// execute_tool span, a weft.repair event carries the original
// arguments mw.RepairJSON replaced (capped at 4 KiB), and a contained
// tool or middleware panic's stack rides its execute_tool span as a
// weft.panic event (16 KiB) — neither under content off. Off by default: a span per layer is 2–6 more spans per
// call for a signal most calls do not need. Like RawBodies it is for
// development only — it takes effect only when the pipeline starts
// under the dev gate (WEFT_ENV=dev read unless NoEnv, or DevMode);
// otherwise Start logs one WARN ("otel: DevDetail ignored: …") and the
// pipeline behaves as without it — and only first-party destinations
// with content on receive the detail: any other destination gets the
// run without its layer spans and without the original arguments. The gate is the pipeline's
// LoggerProvider marker (LoggerProvider.DevDetail), which the core asks:
// pass Pipeline.LoggerProvider() to weft.LoggerProvider, or rely on
// Install's global registration. With the option the agent knows the
// level at New and builds the layer boundaries into its one model
// chain. Through the global provider it cannot (the pipeline may
// register after New): the first run builds a second chain, so the
// WrapModel constructors run twice — a stateful middleware holds two
// states — and a caller streaming Agent.Model() itself (weft/thread's
// compaction summary) gets the plain chain, without layer spans.
func DevDetail() Option {
	return optionFunc(func(c *config) { c.devDetail = true })
}

// NoGlobal leaves the OTel globals alone (Start only; Install always
// registers). For tests and dependency-injected programs. It isolates
// the globals only; NoEnv drops the environment destinations. The value
// is comparable (opt == otel.NoGlobal()).
func NoGlobal() Option { return flagNoGlobal }

// The destinations — any number, all active at once. Each becomes its
// own processor chain with its own queue, drop counters and content
// policy; a slow or failing destination cannot block the others.

// SinkOpener opens a Sink's exporters when the pipeline starts. Either
// may be nil (that signal is not taken); an error fails the
// destination like any other.
type SinkOpener func(ctx context.Context) (sdktrace.SpanExporter, sdklog.Exporter, error)

// Sink is a destination written synchronously: open runs at Start and
// its exporters are driven through simple processors — each record is
// exported before Emit returns, with no queue (a crash loses nothing
// emitted; the export cost rides the run's goroutine). Blocking and
// QueueSize do not apply. Each export is bounded by the destination's
// Timeout (default 2 s): telemetry never blocks control, so a Sink that
// hangs costs the run at most that long — the export is abandoned and
// its records counted dropped, and while it is still stuck every later
// record is dropped at once, until it returns. The deadline is the
// Timeout's alone: a record emitted on a canceled context (a run's tail
// after its cancellation) is still written. A Sink whose export can wait
// longer than 2 s by design — a SQLite store with a long busy_timeout
// behind another writer — should set Timeout to at least that wait, or
// its records are dropped while the store would have taken them. The
// pipeline's Shutdown waits for an abandoned export still running before
// it shuts the exporters down, and leaves them open (an error says so)
// rather than call Shutdown under a running Export. A Sink is first-party (FirstParty): content
// on, raw bodies, the dev detail and app logs eligible. An export error
// is counted as the destination's drop and named by its throttled WARN,
// never handed to OTel's global error handler. The exporters' Shutdown
// runs at the pipeline's Shutdown (after the flush), or at once when
// the pipeline fails to build. name labels the destination's WARN
// lines and drop counter.
func Sink(name string, open SinkOpener, opts ...DestOption) Option {
	return optionFunc(func(c *config) {
		d := newDest(destSink, name)
		d.open = open
		d.firstParty = true
		d.apply(opts)
		c.dests = append(c.dests, d)
	})
}

// Datadog exports to the local Datadog Agent's OTLP intake
// (http://localhost:4318; the Agent binds the standard OTLP ports when
// its OTLP receiver is enabled). Content off, traces + logs, SDK
// default batching. DatadogEndpoint overrides the URL.
func Datadog(opts ...DestOption) Option {
	return optionFunc(func(c *config) {
		d := newDest(destDatadog, "datadog")
		d.apply(opts)
		c.dests = append(c.dests, d)
	})
}

// Langfuse exports traces to Langfuse's OTLP endpoint
// (<host>/api/public/otel/v1/traces, Basic auth base64(pk:sk); OTLP/HTTP
// only — gRPC is not accepted). Traces only: weft's spans carry no
// content (ADR 0016 O7) and Langfuse ingests traces, so it gets timing,
// usage, tool names and the identity chain; span-level content is the
// post-v1 follow-up F1.
func Langfuse(host, publicKey, secretKey string, opts ...DestOption) Option {
	return optionFunc(func(c *config) {
		d := newDest(destLangfuse, "langfuse")
		d.host, d.publicKey, d.secretKey = host, publicKey, secretKey
		d.logs = false
		d.apply(opts)
		c.dests = append(c.dests, d)
	})
}

// OTLP exports to any OTLP/HTTP endpoint (http(s)://host:port). Content
// off, traces + logs, SDK default batching; Headers adds request
// headers. FirstParty marks an endpoint you operate yourself (your own
// collector, a local viewer): content on, raw bodies and app logs
// eligible.
func OTLP(endpoint string, opts ...DestOption) Option {
	return optionFunc(func(c *config) {
		d := newDest(destOTLP, "otlp")
		d.url = endpoint
		d.apply(opts)
		c.dests = append(c.dests, d)
	})
}

// Exporters wires your own exporters (vendor-specific, stdout, tests):
// whichever is non-nil gets a chain. Content off, SDK default batching;
// FirstParty marks them as yours (content on, raw bodies and app logs
// eligible). Sink is the synchronous form.
func Exporters(spans sdktrace.SpanExporter, logs sdklog.Exporter, opts ...DestOption) Option {
	return optionFunc(func(c *config) {
		d := newDest(destExporters, "exporters")
		d.spanExp, d.logExp = spans, logs
		if spans == nil {
			d.traces = false
		}
		if logs == nil {
			d.logs = false
		}
		d.apply(opts)
		c.dests = append(c.dests, d)
	})
}

// DestOption configures one destination.
type DestOption interface{ applyDest(*dest) }

type destOptionFunc func(*dest)

func (f destOptionFunc) applyDest(d *dest) { f(d) }

// WithContent turns content on for this destination, optionally
// overriding the pipeline's ContentConfig.
func WithContent(cfg ...ContentConfig) DestOption {
	return destOptionFunc(func(d *dest) {
		on := true
		d.content = &on
		if len(cfg) > 0 {
			c := cfg[0]
			d.contentCfg = &c
		}
	})
}

// NoContent turns content off: the chain strips event and delta bodies
// (core.StripContent) and drops messages records before its exporter.
func NoContent() DestOption {
	return destOptionFunc(func(d *dest) {
		on := false
		d.content = &on
	})
}

// Signals selects which signals this destination takes. The defaults
// are per destination (Langfuse is traces only; the rest both).
func Signals(traces, logs bool) DestOption {
	return destOptionFunc(func(d *dest) {
		d.traces, d.logs = traces, logs
	})
}

// NoDeltas drops text/reasoning/args deltas before export, to save
// bandwidth when live streaming is not wanted. Deltas are never stored
// by the sinks either way (Q4); the core's durable sequence is
// untouched (they number on their own counter, D3).
func NoDeltas() DestOption {
	return destOptionFunc(func(d *dest) { d.noDeltas = true })
}

// BatchDelay sets the destination's batch flush interval, for logs and
// spans both (default: the SDK's).
func BatchDelay(d time.Duration) DestOption { return BatchDelays(d, d) }

// BatchDelays sets the destination's log and span batch flush
// intervals apart (0 keeps the SDK's default for that signal) — a
// live viewer wants records sooner than spans.
func BatchDelays(logs, spans time.Duration) DestOption {
	return destOptionFunc(func(dd *dest) { dd.logDelay, dd.spanDelay = logs, spans })
}

// FirstParty marks a destination as one the program's operator runs
// (a local store, an in-house viewer): content on by default (NoContent
// still turns it off), and eligible for RawBodies' raw records,
// DevDetail's layer spans and the slog bridge's app logs (AppLogs) —
// what never goes to a third party by default. Every Sink is
// first-party; on OTLP and Exporters it is the opt-in.
func FirstParty() DestOption {
	return destOptionFunc(func(d *dest) { d.firstParty = true })
}

// Name sets the destination's label in its WARN lines and drop counter
// (default: the kind's name — "otlp", "datadog", …).
func Name(name string) DestOption {
	return destOptionFunc(func(d *dest) {
		if name != "" {
			d.name = name
		}
	})
}

// Headers adds HTTP headers to the destination's requests (OTLP-family
// destinations).
func Headers(h map[string]string) DestOption {
	return destOptionFunc(func(d *dest) {
		if d.headers == nil {
			d.headers = map[string]string{}
		}
		for k, v := range h {
			d.headers[k] = v
		}
	})
}

// Timeout sets the destination's export request timeout: an OTLP
// request's, or a Sink's per-export deadline (default 2 s; past it the
// export is abandoned and its records counted dropped). A Sink backed by
// a store that may wait on a lock — SQLite with a busy_timeout — needs a
// Timeout no shorter than that wait.
func Timeout(d time.Duration) DestOption {
	return destOptionFunc(func(dd *dest) { dd.timeout = d })
}

// Insecure allows http:// to a non-loopback host (OTLP-family
// destinations). Loopback http is always allowed, and so is the
// environment's OTEL_EXPORTER_OTLP_ENDPOINT written with http:// (the
// operator's opt-in; see envDestinations).
func Insecure() DestOption {
	return destOptionFunc(func(d *dest) { d.insecure = true })
}

// DatadogEndpoint overrides the Datadog destination's OTLP intake URL
// (default http://localhost:4318).
func DatadogEndpoint(url string) DestOption {
	return destOptionFunc(func(d *dest) { d.url = url })
}

// QueueSize sets how many records (and, separately, spans) the
// destination holds between emission and export — its batch queue
// (default the SDK's 2,048; n <= 0 keeps the default). A full queue never blocks the app: it refuses the
// record, deltas first (at half full), then ordinary records (at seven
// eighths), a run's run_start and run_finish last. Every refusal is
// counted: one WARN per destination ("weft/otel: destination queue
// full"), the count on the heartbeat and every run's run_start and
// run_finish (weft.records.dropped), and the run's gap, which a viewer
// counts against weft.run.events. A Sink has no queue (it writes
// synchronously) and ignores it. The OTEL_BLRP_MAX_QUEUE_SIZE and
// OTEL_BSP_MAX_QUEUE_SIZE variables do not apply to weft's
// destinations.
func QueueSize(n int) DestOption {
	return destOptionFunc(func(d *dest) { d.queueSize = n })
}

// Blocking makes a full queue wait for room instead of refusing a
// durable record — up to 5 s and never past the emitting run's context
// (a span's End, which has none, waits under its own 5 s deadline);
// deltas are still shed. The bound is per stall: once a wait times out
// (the exporter is stuck), every later record is refused at once,
// counted, until an export begins again — a hung exporter costs the app
// one 5 s wait, not one per record. Development only, like
// RawBodies: it takes effect only when the pipeline starts under the
// dev gate (WEFT_ENV=dev read unless NoEnv, or DevMode — which an
// explicit WEFT_ENV other than dev overrules); otherwise
// Start logs one WARN naming the cause ("otel: Blocking ignored:
// WEFT_ENV is not dev", or "… otel.NoEnv reads no WEFT_ENV") and the
// destination refuses as without it — observability never slows a
// production app. A Sink ignores it (no queue).
func Blocking() DestOption {
	return destOptionFunc(func(d *dest) { d.blocking = true })
}

// AppLogs says whether this destination takes the slog bridge's app
// log records (Slog, SlogHandler): on by default for a first-party
// destination (FirstParty, Sink) with content on, off for the rest —
// and for a first-party destination under NoContent — because an app's
// log lines may carry anything it logged, prompts included. AppLogs(true)
// is the opt-in: it admits the lines, never bypasses the destination's
// content policy — an app log line is content like a transcript record,
// so a content-on destination applies ContentConfig's Redact
// (ContentLog) and MaxBytes to its message and string attributes, and a
// content-off one receives it stripped (message and string values
// emptied, weft.content=stripped; severity, span and the other
// attributes kept). AppLogs(false) keeps them from a first-party
// destination. Only a destination that takes the lines carries the
// bridge mark (weft.logs.bridged on its heartbeats and root spans).
// Records the app emits through the OTel Logs API itself are not the
// bridge's and are not governed by it.
func AppLogs(on bool) DestOption {
	return destOptionFunc(func(d *dest) { d.appLogs = &on })
}
