#!/usr/bin/env bash
#
# Summarize the latency/QPS results from a BenchmarkPointSelect run.
#
# Usage:
#   summarize.sh <results-dir>                 # one run
#   summarize.sh <baseline-dir> <variant-dir>  # side-by-side comparison
#
# Each <dir> is a directory containing a `raw.txt` file produced by
# run-baseline.sh (the format is the standard `go test -bench` output
# with our extra p50_us / p99_us metrics from b.ReportMetric).
#
# What you get:
#   1. A per-concurrency table: median (across the --count repeats) of
#      QPS, p50_us, p99_us.
#   2. A "max QPS at p99 budget" table for a fixed set of budgets. This
#      is the bottom line of the experiment — at any budget, what is
#      the highest sustainable QPS? With one arg we just print it; with
#      two we print baseline | variant | delta.
#
# Implementation note: median is the (n+1)/2-th element of a sorted
# list of n samples (ties broken downward at even n). With 5 samples
# this is just the middle one, which is all we ever expect to see, but
# the code does the right thing for any n.

set -euo pipefail

# Latency budgets to report (microseconds). Edit here if you want to
# add or change budgets — affects all three sections.
BUDGETS_US=(200 500 1000 2000 5000)

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <results-dir> [<results-dir>]" >&2
  exit 2
fi

# parse_one <raw.txt> emits one tab-separated line per concurrency level:
#   <conc>\t<median_qps>\t<median_p50_us>\t<median_p99_us>
# Sorted ascending by concurrency.
parse_one() {
  local raw="$1"
  if [[ ! -f "${raw}" ]]; then
    echo "missing: ${raw}" >&2
    exit 1
  fi
  # Portable (BSD/macOS) awk: no match() captures, no asort().
  # We sort within awk via insertion sort, which is fine for n<=10.
  awk '
    # Match lines like:
    #   BenchmarkPointSelect/conc=8-8     587955     20080 ns/op    151.0 p50_us    337.9 p99_us
    /^BenchmarkPointSelect\/conc=/ {
      # $1 = "BenchmarkPointSelect/conc=8-8".
      # Extract the digits between "conc=" and the next "-".
      s = $1
      i = index(s, "conc=")
      if (i == 0) next
      rest = substr(s, i + 5)
      j = index(rest, "-")
      conc = (j == 0) ? (rest + 0) : (substr(rest, 1, j - 1) + 0)
      # Field positions are stable: $3=ns/op, $5=p50_us, $7=p99_us.
      n = ++count[conc]
      nsop[conc, n] = $3 + 0
      p50[conc, n]  = $5 + 0
      p99[conc, n]  = $7 + 0
    }
    # ssort copies n samples into a, sorts ascending in place via
    # insertion sort, returns the median (the (n+1)/2-th sample,
    # tiebroken downward at even n).
    function ssort(a, n,    i, j, t) {
      for (i = 2; i <= n; i++) {
        t = a[i]; j = i - 1
        while (j > 0 && a[j] > t) { a[j+1] = a[j]; j-- }
        a[j+1] = t
      }
      return a[int((n + 1) / 2)]
    }
    END {
      # Walk concurrencies in ascending order.
      ncs = 0
      for (c in count) cs[++ncs] = c + 0
      ssort(cs, ncs)
      for (k = 1; k <= ncs; k++) {
        c = cs[k]
        n = count[c]
        for (i = 1; i <= n; i++) {
          ns_arr[i] = nsop[c, i]
          p50_arr[i] = p50[c, i]
          p99_arr[i] = p99[c, i]
        }
        med_ns  = ssort(ns_arr, n)
        med_p50 = ssort(p50_arr, n)
        med_p99 = ssort(p99_arr, n)
        qps = (med_ns > 0) ? (1e9 / med_ns) : 0
        printf "%d\t%.0f\t%.1f\t%.1f\n", c, qps, med_p50, med_p99
      }
    }
  ' "${raw}"
}

# qps_at_budget <parsed-stream> <budget_us>  →  prints integer QPS
# Walks the (conc, qps, p50, p99) lines (sorted by ascending conc) and
# returns the maximum QPS for which p99 ≤ budget. Returns 0 if the
# smallest concurrency point already exceeds budget.
qps_at_budget() {
  local budget="$2"
  echo "$1" | awk -v B="${budget}" '
    BEGIN { best = 0 }
    {
      if ($4 + 0 <= B + 0 && $2 + 0 > best) best = $2 + 0
    }
    END { printf "%d\n", best }
  '
}

print_concurrency_table() {
  local label="$1" data="$2"
  echo
  echo "## ${label} — per-concurrency medians"
  echo
  printf "| conc | qps    | p50 (µs) | p99 (µs) |\n"
  printf "|----: | -----: | -------: | -------: |\n"
  echo "${data}" | awk -F'\t' '{
    printf "| %4d | %6d | %8.1f | %8.1f |\n", $1, $2, $3, $4
  }'
}

print_budget_table_one() {
  local label="$1" data="$2"
  echo
  echo "## ${label} — max QPS at p99 budget"
  echo
  printf "| p99 budget | max QPS |\n"
  printf "| ---------: | ------: |\n"
  for b in "${BUDGETS_US[@]}"; do
    local q
    q="$(qps_at_budget "${data}" "${b}")"
    printf "| %7d µs | %7d |\n" "${b}" "${q}"
  done
}

print_budget_table_diff() {
  local data_a="$1" data_b="$2"
  echo
  echo "## Max QPS at p99 budget — baseline vs variant"
  echo
  printf "| p99 budget | baseline | variant | delta | delta %% |\n"
  printf "| ---------: | -------: | ------: | ----: | ------: |\n"
  for b in "${BUDGETS_US[@]}"; do
    local qa qb d pct
    qa="$(qps_at_budget "${data_a}" "${b}")"
    qb="$(qps_at_budget "${data_b}" "${b}")"
    d=$(( qb - qa ))
    if [[ "${qa}" -gt 0 ]]; then
      pct="$(awk -v a="${qa}" -v b="${qb}" 'BEGIN { printf "%+.1f", (b - a) * 100.0 / a }')"
    else
      pct="n/a"
    fi
    printf "| %7d µs | %8d | %7d | %5d | %6s%% |\n" "${b}" "${qa}" "${qb}" "${d}" "${pct}"
  done
}

# --- main ---

base_dir="$1"
base_data="$(parse_one "${base_dir}/raw.txt")"

if [[ $# -eq 1 ]]; then
  print_concurrency_table "$(basename "${base_dir}")" "${base_data}"
  print_budget_table_one  "$(basename "${base_dir}")" "${base_data}"
  exit 0
fi

variant_dir="$2"
variant_data="$(parse_one "${variant_dir}/raw.txt")"

print_concurrency_table "baseline ($(basename "${base_dir}"))" "${base_data}"
print_concurrency_table "variant  ($(basename "${variant_dir}"))" "${variant_data}"
print_budget_table_diff "${base_data}" "${variant_data}"
