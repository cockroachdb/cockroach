# Shell support functions and variables. Bash-only.

# Terminal color codes.
term_reset=$(tput sgr0 2>/dev/null || true)
term_red=$(tput setaf 1 2>/dev/null || true)
term_yellow=$(tput setaf 3 2>/dev/null || true)

# `retry COMMAND [ARGS...]` invokes COMMAND with the specified ARGS, retrying it
# if it fails with exponential backoff up to three times.
retry() {
  n=3
  for (( i = 0; i < $n; i++ )); do
    if (( i > 0 )); then
      wait=$((5 * 2**(i-1))) # 5s, 10s
      echo "$term_yellow[$i/$n] $1 failed: retrying in ${wait}s$term_reset"
      sleep "$wait"
    fi
    if "$@"; then
      return 0
    fi
  done
  echo "$term_red[$n/$n] $1 failed: giving up$term_reset"
  return 1
}

# `die [ARGS...]` outputs its arguments to stderr, then exits the program with
# a failing exit code.
die() {
  echo "$@" >&2
  exit 1
}
