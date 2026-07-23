#!/usr/bin/env bash
# Coverage gate (the vitest-thresholds equivalent for this repo): runs the
# race-enabled suite with coverage and fails if the total or any core
# package drops below its floor.
#
# Floors are ratchets: they defend a reached state. When coverage rises,
# raise the floor to just under the new value; never lower one to get green.
#
# Portable to macOS's stock bash 3.2 — no associative arrays.
set -euo pipefail
cd "$(dirname "$0")/.."

TOTAL_FLOOR=55

# floor_for <pkg> — per-package floors for core packages. cmd/ (cobra
# wiring), internal/runner (real docker; covered by E2E) and internal/tui
# (bubbletea view) are exempt from floors but still counted in the total.
floor_for() {
  case "$1" in
    internal/broker) echo 60 ;;
    internal/bus) echo 95 ;;
    internal/config) echo 70 ;;
    internal/control) echo 70 ;;
    internal/githubauth) echo 75 ;;
    internal/metrics) echo 60 ;;
    internal/slots) echo 90 ;;
    *) echo "" ;;
  esac
}

profile=$(mktemp)
trap 'rm -f "$profile"' EXIT
go test -race -coverprofile="$profile" ./... >/dev/null

fail=0
while IFS= read -r line; do
  pkg=$(awk '{for (i=1;i<=NF;i++) if ($i ~ /^github.com/) print $i}' <<<"$line" | sed 's#github.com/roark-dev/deckhand/##')
  pct=$(sed -n 's/.*coverage: \([0-9.]*\)% of statements.*/\1/p' <<<"$line")
  [ -z "$pkg" ] || [ -z "$pct" ] && continue
  floor=$(floor_for "$pkg")
  if [ -n "$floor" ] && awk -v p="$pct" -v f="$floor" 'BEGIN{exit !(p<f)}'; then
    echo "FAIL  $pkg ${pct}% < floor ${floor}%"
    fail=1
  else
    printf '  ok  %-22s %s%%\n' "$pkg" "$pct"
  fi
done < <(go test -cover ./... 2>/dev/null | grep 'coverage:')

total=$(go tool cover -func="$profile" | awk '/^total:/ {gsub(/%/,"",$3); print $3}')
if awk -v t="$total" -v f="$TOTAL_FLOOR" 'BEGIN{exit !(t<f)}'; then
  echo "FAIL  total ${total}% < floor ${TOTAL_FLOOR}%"
  fail=1
else
  echo "  ok  total                  ${total}%"
fi

exit $fail
