package otel

import (
	"runtime/debug"
	"testing"
)

// The resource's version-control stamp comes from the build info the
// go command embeds: a revision and its modified flag, nothing without
// a revision. No digest, no hashing.
func TestVCSAttrs(t *testing.T) {
	got := vcsAttrsOf([]debug.BuildSetting{{Key: "vcs.revision", Value: "deadbeef"}, {Key: "vcs.modified", Value: "true"}})
	if len(got) != 2 || got[0].Value.AsString() != "deadbeef" || !got[1].Value.AsBool() {
		t.Fatalf("vcs attrs = %v", got)
	}
	if got := vcsAttrsOf([]debug.BuildSetting{{Key: "vcs.modified", Value: "true"}}); got != nil {
		t.Fatalf("no revision, attrs = %v", got)
	}
	r, err := buildResource(config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Set().Value("weft.build.digest"); ok {
		t.Fatal("the resource carries a build digest: weft never hashes the binary")
	}
}
