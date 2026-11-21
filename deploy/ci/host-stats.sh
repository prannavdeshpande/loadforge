#!/usr/bin/env bash
# Appends a snapshot of the agent's CPU, memory and IO pressure, memory and
# swap to reports/host-stats.txt, labelled with $1 (e.g. "unit:before").
#
# The timing tests assert on dispatch lag; a host that swaps or is
# descheduled fails them without any code being wrong. Comparing the
# "before" and "after" blocks of a failed stage (pressure totals, swap-in
# and swap-out counts) shows whether the host stalled during it. Read it
# with the host-stall monitor line timingtest logs on a failed test.
#
# It only observes: a missing source prints "unavailable", and the script
# always exits 0 so it can never fail a build.
set -u

label=${1:-unlabelled}
out=${HOST_STATS_FILE:-reports/host-stats.txt}
mkdir -p "$(dirname "$out")" 2>/dev/null || true

show() { # show <title> <file>
  printf -- '-- %s\n' "$1"
  if [ -r "$2" ]; then cat "$2"; else echo "unavailable"; fi
}

{
  printf '==== %s  %s  host=%s\n' "$label" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(hostname 2>/dev/null || echo unknown)"
  printf -- '-- uptime / load\n'
  uptime 2>/dev/null || echo "unavailable"
  show "pressure cpu" /proc/pressure/cpu
  show "pressure memory" /proc/pressure/memory
  show "pressure io" /proc/pressure/io
  printf -- '-- free -m\n'
  free -m 2>/dev/null || echo "unavailable"
  printf -- '-- swap (/proc/meminfo)\n'
  if [ -r /proc/meminfo ]; then grep -E '^(SwapTotal|SwapFree|SwapCached):' /proc/meminfo; else echo "unavailable"; fi
  printf -- '-- swap activity since boot (/proc/vmstat, pages)\n'
  if [ -r /proc/vmstat ]; then grep -E '^(pswpin|pswpout) ' /proc/vmstat; else echo "unavailable"; fi
  echo
} >>"$out" 2>&1

exit 0
