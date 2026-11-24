#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

if [ -z "$(which gdate)" ]; then
	echo "gdate not found, run: brew install coreutils"
	exit 1
fi

if [ -z "$(which jq)" ]; then
	echo "jq not found, run: brew install jq"
	exit 1
fi

if [ -z "${1-}" ]; then
  cat <<EOF
Usage: $(basename "$0") <base date>

<base date> should be the date of the Monday following the L3 shift. You can use
'today' if it is today.

This should be run on the Monday following the L3 shift to generate an
up-to-date summary of the various queues, ideally immediately before the on-call
meeting.

EOF
	exit 1
fi

base="${1}"

# We care about the recently completed interval
# which started Saturday last week and finished
# Friday EOD, i.e. Sat <= t <= Fri.
#
# To a lesser degree, we also care about what came
# in since the new shift # started, i.e. Sat2 <= t <= Sun.
# The assignee will not necessarily have had time to work
# on these.
#
# [Sat1] Sun Mon Tues Wed Thurs [Fri] [Sat2] [Sun]
sat1=$(gdate -d "${base} - 9 days" "+%Y-%m-%d")
fri=$(gdate -d "${base} - 3 days" "+%Y-%m-%d")
#sat2=$(gdate -d "${base} - 3 days" "+%Y-%m-%d")
#sun=$(gdate -d "${base} - 1 days" "+%Y-%m-%d")

function count() {
  # args: all|open|closed github_query
  gh issue list --json number,title -L 999 -s "${1}" -q 'length' -S "$2"
}

function ghurl() {
  echo -n "https://github.com/cockroachdb/cockroach/issues?q="
  echo -n "$1" | jq -sRr @uri
}

function mdghurl() {
  echo -n "[${1}]($(ghurl "${2}"))"
}




q_open_rel_blockers="is:issue is:open label:C-test-failure label:release-blocker,GA-blocker label:T-kv,T-kv-replication,T-admission-control"
open_rel_blockers_count=$(count open "${q_open_rel_blockers}")
relblockersmd=$(mdghurl "Release Blockers" "${q_open_rel_blockers}")

q_roachtest_queue="is:issue label:O-roachtest label:T-kv,T-kv-replication,T-admission-control -label:C-bug,X-infra-flake,X-duplicate,X-unactionable,X-invalid,X-stale"
roachtest_queue_count=$(count all "${q_roachtest_queue}")
roachtestqueuemd=$(mdghurl "Roachtest Triage" "${q_roachtest_queue}")

q_triage_queue="is:issue label:O-robot -label:O-roachtest label:C-test-failure label:T-kv,T-kv-replication,T-admission-control -label:C-bug,X-infra-flake,X-duplicate,X-unactionable,X-invalid,X-stale"
triage_queue_count=$(count all "${q_triage_queue}")
triagequeuemd=$(mdghurl "Unit Test Triage" "${q_triage_queue}")

q_open_test_failures="is:issue is:open label:C-test-failure,skipped-test label:T-kv,T-kv-replication,T-admission-control"
open_test_failures_count=$(count open "${q_open_test_failures}")
q_open_test_failures_dedup="is:issue is:open label:C-test-failure,skipped-test label:T-kv,T-kv-replication,T-admission-control -label:X-infra-flake -label:X-duplicate -label:X-unactionable -label:X-invalid -label:X-stale -label:O-perturbation"
open_test_failures_dedup_count=$(count open "${q_open_test_failures_dedup}")
openfailuresmd=$(mdghurl "Open Test Failures" "${q_open_test_failures_dedup}")

# Github `..` issue filtering is inclusive on the end day.
ghweek="${sat1}..${fri}"

q_created_in_week="created:$ghweek is:issue label:C-test-failure label:T-kv,T-kv-replication,T-admission-control"
created=$(count all "${q_created_in_week}")
createdmd=$(mdghurl "Created" "${q_created_in_week}")

q_closed_in_week="closed:$ghweek is:issue label:C-test-failure label:T-kv,T-kv-replication,T-admission-control"
closed=$(count all "${q_closed_in_week}")
closedmd=$(mdghurl "Closed" "${q_closed_in_week}")

from=$(gdate -d "${sat1}" +"%a %Y-%m-%d")
to=$(gdate -d "${fri}" +"%a %Y-%m-%d")
now=$(env TZ='America/New_York' gdate +"%a %Y-%m-%d %H:%M:%S ET")

cat <<EOF
- Test failure trends over ${from} to ${to} (incl):
	- ${createdmd}: ${created}
	- ${closedmd}: ${closed}
	- Net change: $((created-closed))
- Queues (as of $now):
	- ${relblockersmd}: $open_rel_blockers_count
	- ${roachtestqueuemd}: $roachtest_queue_count
	- ${triagequeuemd}: $triage_queue_count
	- ${openfailuresmd}: $open_test_failures_dedup_count (+$((open_test_failures_count - open_test_failures_dedup_count)) duplicates)
EOF

