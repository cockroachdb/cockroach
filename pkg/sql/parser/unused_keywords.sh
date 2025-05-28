#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Usage: unused_keywords.sh pkg/*.y

set -euo pipefail

GRAMMAR_FILE="$1"

if [ -z "$1" ]; then
  echo "Usage: $0 <input_grammar_file>"
  exit 1
fi

# Find the line in the grammar file where different keyword groups begin - we
# don't want to count occurrences from that point.
line=$(grep -n -m 1 "^unreserved_keyword:$" "$GRAMMAR_FILE" | cut -d: -f1)

# Find all non-reserved keywords.
awk -f <(cat <<'EOF'
/^(cockroachdb_extra_)?reserved_keyword:/ {
  keyword = 0
  next
}
/^.*_keyword:/ {
  keyword = 1
  next
}
/^$/ {
  keyword = 0
}
{
  if (keyword && $NF != "") {
    print $NF
  }
}
EOF
) < "$GRAMMAR_FILE" | while read -r kw; do
  # Count number of times the non-reserved keyword appears before the keyword
  # groups - if it's present only once, then it's not actually used and can be
  # removed from the grammar.
  #
  # Note that this could produce false positives in case these keywords are used
  # somewhere outside the grammar (for example, in the lexer).
  count=$(head -n "$line" "$GRAMMAR_FILE" | grep -c "$kw")
  if [ "$count" -le 1 ]; then
    echo "$kw"
  fi
done
