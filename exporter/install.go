package exporter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/weftgo/weft/internal/discovery"
	"github.com/weftgo/weft/obsdb"
	"github.com/weftgo/weft/otel"
)

// envGetenv is the package's one environment seam (tests swap it).
var envGetenv = os.Getenv

type markerKind int

const (
	kindLocal markerKind = iota + 1
	kindStudio
	kindNoDiscovery
)

// marker is what this package knows of an option it handed out
// (otel.Options values are comparable): Install and Start recognise
// their Local, Studio and NoDiscovery options among the ones they are
// given.
type marker struct {
	opt        otel.Option
	kind       markerKind
	url, token string       // Studio
	handle     *localHandle // Local: its open database, under regMu
}

// The registry. markers grows by one per Local/Studio/NoDiscovery call
// — options are built a handful of times per process.
var (
	regMu      sync.Mutex
	markers    []*marker
	openLocals []*localHandle // every open Local database, oldest first
	started    = map[*otel.Pipeline]*startInfo{}
)

// startInfo is what Install or Start configured for one pipeline.
type startInfo struct {
	locals     []*marker
	url, token string
}

func register(m *marker) {
	regMu.Lock()
	markers = append(markers, m)
	regMu.Unlock()
}

// find returns the marker of an option this package handed out. The
// comparison never panics: every marker's option is a pointer, and an
// interface comparison across dynamic types is false.
func find(o otel.Option) *marker {
	regMu.Lock()
	defer regMu.Unlock()
	for _, m := range markers {
		if m.opt == o {
			return m
		}
	}
	return nil
}

// NoDiscovery makes Install and Start skip the discovery file a running
// `weft studio` / `weft dev` wrote, and nothing else: WEFT_STUDIO_URL,
// the OTEL_* variables and the WEFT_ENV development gate are read as
// usual (otel.NoEnv drops them all). The per-pipeline form of
// WEFT_DISCOVERY=off. A pipeline that names a Studio or a Local needs
// no NoDiscovery: either turns the read off on its own.
func NoDiscovery() otel.Option {
	m := &marker{kind: kindNoDiscovery, opt: otel.Options()}
	register(m)
	return m.opt
}

// Start is otel.Start with the platform's environment: WEFT_STUDIO_URL
// and WEFT_STUDIO_TOKEN add a Studio destination (content on), and,
// when they are unset and the options name no Local and no Studio,
// the discovery file of a running `weft studio` / `weft dev` stands in
// for them (unless NoDiscovery, otel.NoEnv or WEFT_DISCOVERY=off). It
// adds no fallback: with nothing configured it returns
// otel.ErrNoDestinations.
func Start(ctx context.Context, opts ...otel.Option) (*otel.Pipeline, error) {
	all, info, joined := plan(opts, false)
	p, err := otel.Start(ctx, all...)
	if err != nil {
		return nil, err
	}
	remember(p, info, joined)
	return p, nil
}

// Install is otel.Install with Start's environment, plus the
// zero-config rule: with no destination configured (in code, the
// OTEL_* variables, WEFT_STUDIO_URL or the discovery file) it writes
// Local("") — $WEFT_DB or ./.weft/weft.db — and so it does, with one
// WARN, when only environment destinations were configured and none
// could be built. Like otel.Install it never fails the program; the
// returned function flushes and shuts everything down.
func Install(opts ...otel.Option) func() {
	all, info, joined := plan(opts, true)
	stop := otel.Install(all...)
	if p := otel.Installed(); p != nil {
		remember(p, info, joined)
		return func() {
			stop()
			regMu.Lock()
			delete(started, p)
			regMu.Unlock()
		}
	}
	return stop
}

// plan adds the platform's environment destinations (and, for Install,
// the Local fallback) to opts.
func plan(opts []otel.Option, fallback bool) (all []otel.Option, info *startInfo, joined *discovered) {
	info = &startInfo{}
	noEnv, noDisc, named := false, false, false
	var explicit []string // the explicit Studios' URLs, for de-duplication
	for _, o := range opts {
		if o == otel.NoEnv() {
			noEnv = true
		}
		m := find(o)
		if m == nil {
			continue
		}
		switch m.kind {
		case kindLocal:
			info.locals = append(info.locals, m)
			named = true
		case kindStudio:
			info.url, info.token = m.url, m.token
			explicit = append(explicit, dedupURL(m.url))
			named = true
		case kindNoDiscovery:
			noDisc = true
		}
	}
	all = append([]otel.Option(nil), opts...)
	if !noEnv {
		getenv := envGetenv
		// Explicit > environment > file: a Studio or a Local named in
		// code switches the discovery read off, as WEFT_STUDIO_URL and
		// NoDiscovery do — the file never adds a second Studio, never
		// writes runs into a store the caller did not name, and never
		// becomes the one StudioEndpoint (and runtime.Install) reports.
		if !noDisc && !named {
			getenv, joined = discoverStudio(getenv)
		}
		if u := getenv("WEFT_STUDIO_URL"); u != "" {
			u = normalizeEndpoint(u)
			dup := false
			for _, e := range explicit {
				dup = dup || e == dedupURL(u)
			}
			if err := envStudioURL(u); err != nil {
				// Refused here rather than by otel: an environment
				// destination that cannot be built leaves Install its
				// Local fallback (otel falls back only when no
				// destination was named in code).
				slog.Warn("weft/exporter: WEFT_STUDIO_URL skipped", "err", err.Error())
			} else if !dup {
				tok := getenv("WEFT_STUDIO_TOKEN")
				all = append(all, Studio(u, tok, otel.Name("studio(env)")))
				info.url, info.token = u, tok
			}
		}
	}
	if fallback {
		l := Local("")
		info.locals = append(info.locals, find(l))
		all = append(all, otel.Fallback(l))
	}
	return all, info, joined
}

func remember(p *otel.Pipeline, info *startInfo, joined *discovered) {
	regMu.Lock()
	started[p] = info
	regMu.Unlock()
	if joined != nil && info.url == normalizeEndpoint(joined.info.URL) {
		// The one INFO line: which Studio this process joined, and how
		// to stop it doing so.
		slog.Info("weft/exporter: joined the running Studio", "url", joined.info.URL,
			"pid", joined.info.PID, "file", joined.path,
			"hint", "WEFT_STUDIO_URL names another; exporter.NoDiscovery() or WEFT_DISCOVERY=off ignores the file")
	}
}

// LocalDB returns the installed pipeline's Local database — for
// studio.DB(exporter.LocalDB()), the handle setup A shares with Studio.
// For a pipeline Install or Start built, it is that pipeline's Local
// (the last one configured); for one otel.Install built from a Local
// option, the most recently opened Local database still open. nil
// before an install, after its shutdown, or without a Local.
func LocalDB() obsdb.DB {
	p := otel.Installed()
	if p == nil {
		return nil
	}
	if db := LocalDBOf(p); db != nil {
		return db
	}
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := started[p]; ok || len(openLocals) == 0 {
		return nil
	}
	return openLocals[len(openLocals)-1].db
}

// LocalDBOf returns the Local database of a pipeline Install or Start
// built (the last Local configured that is open), else nil — a
// pipeline started under otel.NoGlobal included.
func LocalDBOf(p *otel.Pipeline) obsdb.DB {
	regMu.Lock()
	defer regMu.Unlock()
	info := started[p]
	if info == nil {
		return nil
	}
	for i := len(info.locals) - 1; i >= 0; i-- {
		if h := info.locals[i].handle; h != nil && h.isOpen() {
			return h.db
		}
	}
	return nil
}

// StudioEndpoint returns the installed pipeline's Studio destination
// (url and token), which weft/runtime dials by default: the
// environment's (WEFT_STUDIO_URL or the discovery file) when it added
// one, else the last Studio option. Known for a pipeline Install or
// Start built; "" otherwise, before an install and after its shutdown.
func StudioEndpoint() (string, string) {
	p := otel.Installed()
	if p == nil {
		return "", ""
	}
	return StudioEndpointOf(p)
}

// StudioEndpointOf is StudioEndpoint for a pipeline Install or Start
// built, installed or not.
func StudioEndpointOf(p *otel.Pipeline) (string, string) {
	regMu.Lock()
	defer regMu.Unlock()
	if info := started[p]; info != nil {
		return info.url, info.token
	}
	return "", ""
}

// discovered is the running Studio the discovery file named.
type discovered struct {
	info discovery.Info
	path string
}

// discoverStudio is the discovery rung: when WEFT_STUDIO_URL is unset
// and WEFT_DISCOVERY is not "off", the discovery file `weft studio` /
// `weft dev` writes (internal/discovery: ./.weft, then
// $XDG_RUNTIME_DIR/weft, then the user cache directory) stands in for
// WEFT_STUDIO_URL and WEFT_STUDIO_TOKEN — its url and token, together.
// A stale file (its pid gone, or 24 hours old) is never trusted; a
// missing, stale or malformed one changes nothing and logs Debug at
// most.
func discoverStudio(getenv func(string) string) (func(string) string, *discovered) {
	info, path, ok := discovery.Lookup(getenv, slog.Default())
	if !ok {
		return getenv, nil
	}
	return func(k string) string {
		switch k {
		case "WEFT_STUDIO_URL":
			return info.URL
		case "WEFT_STUDIO_TOKEN":
			return info.Token
		}
		return getenv(k)
	}, &discovered{info: info, path: path}
}

// dedupURL is the URL two Studios are compared by: scheme, host and
// path.
func dedupURL(raw string) string {
	u, err := url.Parse(normalizeEndpoint(raw))
	if err != nil || u.Host == "" {
		return raw
	}
	return u.Scheme + "://" + u.Host + strings.TrimSuffix(u.Path, "/")
}

// envStudioURL is the check otel applies to an OTLP endpoint, for the
// environment's Studio: an http(s) URL with a host, http:// only to a
// loopback host — WEFT_STUDIO_URL carries a bearer token, so plaintext
// to another host takes Studio(url, token, otel.Insecure()) in code.
// The errors never echo the raw value: a URL may carry credentials.
func envStudioURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("bad endpoint URL: it does not parse")
	}
	if u.Host == "" {
		return fmt.Errorf("bad endpoint URL %q: no host", u.Redacted())
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		switch u.Hostname() {
		case "localhost", "127.0.0.1", "::1":
			return nil
		}
		return fmt.Errorf("http:// to non-loopback %q needs otel.Insecure() on a Studio named in code", u.Host)
	}
	return fmt.Errorf("endpoint scheme %q: OTLP/HTTP takes http:// or https://", u.Scheme)
}
