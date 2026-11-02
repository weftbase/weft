package otel

import (
	"slices"
	"testing"
)

// Tag: TagOf recognises the option, and a pipeline lists the tag of
// every destination it built — nested in Options and Fallback included,
// a Fallback left unused and a destination Install skipped excluded.
func TestTags(t *testing.T) {
	clearEnvDestinations(t)
	a, b, fb := newMemSink(), newMemSink(), newMemSink()
	optA := Tag("a", a.option())
	if TagOf(optA) != "a" || TagOf(a.option()) != nil || TagOf(Options(optA)) != nil {
		t.Fatal("TagOf does not recognise exactly the option Tag built")
	}
	p, err := Start(testCtx(t), NoGlobal(), Heartbeat(0),
		optA, Options(Tag("outer", Tag("b", b.option()))), Fallback(Tag("fb", fb.option())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(testCtx(t)) }()
	if got, want := p.Tags(), []any{"a", "b", "outer"}; !slices.Equal(got, want) {
		t.Errorf("Tags = %v, want %v (an unused Fallback lists nothing)", got, want)
	}

	// The Fallback's tag, when it is used.
	p2, err := Start(testCtx(t), NoGlobal(), Heartbeat(0), Fallback(Tag("fb", newMemSink().option())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p2.Shutdown(testCtx(t)) }()
	if got := p2.Tags(); !slices.Equal(got, []any{"fb"}) {
		t.Errorf("Tags = %v, want [fb]", got)
	}

	// A destination Install skipped lists no tag.
	shutdown := Install(NoEnv(), Heartbeat(0), Tag("bad", OTLP("ftp://nowhere:1")), Tag("ok", newMemSink().option()))
	defer shutdown()
	if got := Installed().Tags(); !slices.Equal(got, []any{"ok"}) {
		t.Errorf("Tags = %v, want [ok] (the skipped destination's tag is not listed)", got)
	}
}

// Environment: a wrapper's environment destination is de-duplicated
// against an explicit one on the same URL (the explicit one wins),
// dropped under NoEnv, and does not count as named in code — so a
// Fallback takes over when it fails to build under Install.
func TestEnvironment(t *testing.T) {
	clearEnvDestinations(t)
	env := Environment(Tag("env", OTLP("https://studio.example:4318", Name("studio(env)"))))

	p, err := Start(testCtx(t), NoGlobal(), Heartbeat(0), OTLP("https://studio.example:4318/"), env)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Destinations(); !slices.Equal(got, []string{"otlp"}) {
		t.Errorf("Destinations = %v, want the explicit otlp alone (same URL)", got)
	}
	_ = p.Shutdown(testCtx(t))

	p, err = Start(testCtx(t), NoGlobal(), Heartbeat(0), OTLP("https://elsewhere.example:4318"), env)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Destinations(); !slices.Equal(got, []string{"otlp", "studio(env)"}) {
		t.Errorf("Destinations = %v, want both (other URL)", got)
	}
	if got := p.Tags(); !slices.Equal(got, []any{"env"}) {
		t.Errorf("Tags = %v, want [env]", got)
	}
	_ = p.Shutdown(testCtx(t))

	if _, err := Start(testCtx(t), NoGlobal(), NoEnv(), Heartbeat(0), env); err != ErrNoDestinations {
		t.Errorf("Start(NoEnv, Environment(…)) = %v, want ErrNoDestinations", err)
	}

	fb := newMemSink()
	shutdown := Install(Heartbeat(0), Environment(OTLP("ftp://broken:1")), Fallback(Tag("fb", fb.option())))
	defer shutdown()
	if p := Installed(); p == nil || !slices.Equal(p.Tags(), []any{"fb"}) {
		t.Fatal("Install did not fall back when its only environment destination failed")
	}
}
