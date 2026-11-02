package otel

import (
	"time"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// config is what the options built. The zero config plus Start's
// defaults is the whole pipeline: default content 32 KiB, default
// heartbeat 10 s, default sampler ParentBased(AlwaysOn).
type config struct {
	dests []dest
	// fallback is Fallback's destinations: used only when the pipeline
	// would otherwise have none (see Fallback).
	fallback   []dest
	content    ContentConfig
	service    string
	resource   *sdkresource.Resource
	heartbeat  time.Duration // Start seeds the 10s default; <= 0 = disabled
	sampler    sdktrace.Sampler
	noEnv      bool
	noGlobal   bool
	dropFailed bool // Install's WARN-and-skip mode
	// rawBodies is RawBodies' ask; Start keeps it only when the dev
	// gate holds (devGate), else one WARN and off.
	rawBodies bool
	// devDetail is DevDetail's ask; Start keeps it only when the dev
	// gate holds, like rawBodies.
	devDetail bool
	// devMode is DevMode's attestation: the dev gate holds without
	// WEFT_ENV=dev — under NoEnv, or with WEFT_ENV unset (an explicit
	// other value wins).
	devMode bool
	// slog is Slog's ask: wrap slog.Default with the bridge once the
	// globals are registered (never under NoGlobal).
	slog bool
}

type destKind int

const (
	destSink destKind = iota
	destDatadog
	destLangfuse
	destOTLP
	destExporters
)

// dest is one destination's configuration before Start builds it.
type dest struct {
	kind destKind
	name string // the drop-counter and WARN label

	open                       SinkOpener            // Sink
	url                        string                // OTLP/DatadogEndpoint
	host, publicKey, secretKey string                // Langfuse
	spanExp                    sdktrace.SpanExporter // Exporters
	logExp                     sdklog.Exporter       // Exporters
	content                    *bool                 // nil = the kind's default
	contentCfg                 *ContentConfig
	traces, logs               bool
	noDeltas                   bool
	// firstParty is FirstParty's mark (every Sink carries it): a
	// destination the program's operator runs — content on by default,
	// raw bodies, the dev detail and app logs eligible.
	firstParty bool
	// logDelay and spanDelay are BatchDelay's / BatchDelays' ask (0: the
	// SDK's defaults).
	logDelay, spanDelay time.Duration
	headers             map[string]string
	timeout             time.Duration
	insecure            bool
	// queueSize is QueueSize's ask (0: the SDK's 2,048);
	// blocking is Blocking's, kept by Start only under the dev gate
	// (devGate: WEFT_ENV=dev read without NoEnv, or DevMode).
	queueSize int
	blocking  bool
	// appLogs is AppLogs' ask (nil: the kind's default, appLogsOn).
	appLogs *bool
}

func newDest(kind destKind, name string) dest {
	return dest{kind: kind, name: name, traces: true, logs: true}
}

func (d *dest) apply(opts []DestOption) {
	for _, o := range opts {
		o.applyDest(d)
	}
}

// contentDefault is S2.2's table: a first-party destination (a Sink, or
// one marked FirstParty) on, everything else off — content never
// reaches a third party by accident.
func (d dest) contentDefault() bool { return d.firstParty }

// rawOn is whether this destination takes raw records (ADR 0028 §12)
// when the pipeline's RawBodies is in force: a first-party destination
// with content on, never a third party whatever its content setting.
func (d dest) rawOn() bool { return d.firstParty && d.contentOn() }

// appLogsOn is whether this destination takes the slog bridge's app
// log records (AppLogs): AppLogs' answer when given, else a first-party
// destination with content on — an app log may carry anything, prompts
// included, so a third party never gets one by default.
func (d dest) appLogsOn() bool {
	if d.appLogs != nil {
		return *d.appLogs
	}
	return d.firstParty && d.contentOn()
}

func (d dest) contentOn() bool {
	if d.content != nil {
		return *d.content
	}
	return d.contentDefault()
}

// batchDelays returns the log and span batch intervals: BatchDelay's or
// BatchDelays', else zero (the SDK's defaults).
func (d dest) batchDelays() (logs, spans time.Duration) { return d.logDelay, d.spanDelay }

// queueSizeOf is the destination's batch queue size, for its log and
// its span chain each: QueueSize's when given, else the SDK's 2,048. A
// Sink has no queue (written synchronously).
func (d dest) queueSizeOf() int {
	if d.queueSize > 0 {
		return d.queueSize
	}
	return defaultQueueSize
}

// datadogDefaultURL is the Datadog Agent's OTLP/HTTP intake.
const datadogDefaultURL = "http://localhost:4318"
