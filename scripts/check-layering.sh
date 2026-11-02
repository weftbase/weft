#!/bin/sh
# check-layering.sh — the layering guard (WEFTBASE-REPO-SPLIT.md §7):
# weft emits; the platform stores, shows and controls. The platform
# imports the framework, never the reverse. Fails when any framework
# package — its tests included (go list -deps -test) — depends on a
# platform package.
#
# The platform packages (they move to the weftbase repository): studio,
# obsdb, runtime, scope, exporter, cmd/weft, internal/{discovery,
# linkreg,dblock,doctor,listen,termsafe}, examples/studio-local,
# examples/devtools-vite and thread/examples/approvals, each with its
# subpackages. Everything else in the workspace is the framework.
# Paths are matched in full, so the standard library's runtime never
# matches.
#
# Run from the repository root:  scripts/check-layering.sh   (make layering)
set -eu

GO=${GO:-go}
mod=github.com/weftgo/weft
platform="^$mod/(studio|obsdb|runtime|scope|exporter|cmd/weft|internal/(discovery|linkreg|dblock|doctor|listen|termsafe)|examples/(studio-local|devtools-vite)|thread/examples/approvals)(/|\$| |\\.test)"

fail=0
checked=0
for dir in $($GO list -m -f '{{.Dir}}'); do
	# The framework packages of this module.
	pkgs=$(cd "$dir" && $GO list -f '{{.ImportPath}}' ./... | grep -Ev "$platform" || true)
	[ -n "$pkgs" ] || continue
	for p in $pkgs; do
		checked=$((checked + 1))
		bad=$(cd "$dir" && $GO list -deps -test -f '{{.ImportPath}}' "$p" | grep -E "$platform" | sed 's/ .*//' | sort -u || true)
		if [ -n "$bad" ]; then
			fail=1
			echo "$p depends on the platform:"
			echo "$bad" | sed 's/^/    /'
		fi
	done
done

if [ "$fail" -ne 0 ]; then
	echo "layering: framework packages must not import platform packages (scripts/check-layering.sh)" >&2
	exit 1
fi
echo "layering: ok ($checked framework packages, tests included, import no platform package)"
