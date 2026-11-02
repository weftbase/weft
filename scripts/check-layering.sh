#!/bin/sh
# check-layering.sh — the layering guard (WEFTBASE-REPO-SPLIT.md §7):
# weft emits; the platform (github.com/weftbase/weftbase: Studio,
# obsdb, the runtime link, the local exporter, the weftbase CLI)
# stores, shows and controls. The platform imports the framework,
# never the reverse. Fails when any package of this workspace — its
# tests included (go list -deps -test) — depends on a package of the
# platform module.
#
# Run from the repository root:  scripts/check-layering.sh   (make layering)
set -eu

GO=${GO:-go}
platform='^github\.com/weftbase/weftbase(/|$| |\.test)'

fail=0
checked=0
for dir in $($GO list -m -f '{{.Dir}}'); do
	pkgs=$(cd "$dir" && $GO list -f '{{.ImportPath}}' ./...)
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
	echo "layering: weft must not import github.com/weftbase/weftbase (scripts/check-layering.sh)" >&2
	exit 1
fi
echo "layering: ok ($checked packages, tests included, import nothing of github.com/weftbase/weftbase)"
