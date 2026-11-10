#!/bin/sh
# check-layering.sh — the layering guard:
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

# Fail closed: a `go list` that fails must fail the guard, never read
# as "nothing depends on the platform". An assignment's status is
# checked by set -e; a command substitution in a for list, or the left
# side of a pipeline, is not.
dirs=$($GO list -m -f '{{.Dir}}')

fail=0
checked=0
for dir in $dirs; do
	pkgs=$(cd "$dir" && $GO list -f '{{.ImportPath}}' ./...)
	[ -n "$pkgs" ] || continue
	# One go list per module: every package's dependency closure,
	# tests included, as "<package> <dep> <dep> ...".
	deps=$(cd "$dir" && $GO list -deps -test -f '{{.ImportPath}}' ./...)
	checked=$((checked + $(printf '%s\n' "$pkgs" | wc -l)))
	bad=$(printf '%s\n' "$deps" | grep -E "$platform" | sed 's/ .*//' | sort -u || true)
	if [ -n "$bad" ]; then
		fail=1
		echo "module $dir depends on the platform:"
		echo "$bad" | sed 's/^/    /'
		# Name the packages of this workspace that pull it in.
		for p in $pkgs; do
			if (cd "$dir" && $GO list -deps -test -f '{{.ImportPath}}' "$p") | grep -Eq "$platform"; then
				echo "  via $p"
			fi
		done
	fi
done

if [ "$checked" -eq 0 ]; then
	echo "layering: no package was checked (scripts/check-layering.sh)" >&2
	exit 1
fi

if [ "$fail" -ne 0 ]; then
	echo "layering: weft must not import github.com/weftbase/weftbase (scripts/check-layering.sh)" >&2
	exit 1
fi
echo "layering: ok ($checked packages, tests included, import nothing of github.com/weftbase/weftbase)"
