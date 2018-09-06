#!/usr/bin/env bash

set -euo pipefail

tmpdir=$(mktemp -d)

make test PKG="./pkg/sql/opt/norm ./pkg/sql/opt/xform ./pkg/sql/opt/memo/..."  TESTFLAGS=-v\ -ruleprofile | grep "Rule matched:" | sed "s/Rule matched: //" > $tmpdir/rule_names

# Hackily extract the list of every rule name from rule_name.og.go.
cat pkg/sql/opt/rule_name.og.go | grep "^\t[A-Z]" | grep -v "NumRuleNames" | sed "s/[[:space:]]//" > $tmpdir/all_rules

# Subtract one from each rule count via awk since we counted everything an
# extra time to make sure even the rules with 0 uses show up.
cat $tmpdir/rule_names $tmpdir/all_rules | sort | uniq -c | sort -n | awk "{print \$1-1, \$2}"
