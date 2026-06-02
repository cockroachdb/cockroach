#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Counterpart to configure-go-module-auth.sh. Self-hosted runners have
# a persistent HOME, so without this cleanup an expired installation
# token would linger in ~/.gitconfig between jobs.
#
# `git config --unset-all <name>` treats <name> as a literal, so we
# enumerate matching keys with --get-regexp first, then unset each.
# --name-only must precede --get-regexp; anything after the regex is
# parsed as the optional value-regex positional, not as a flag.
# --get-regexp also exits 1 when no entries match — fine here, since
# a clean ~/.gitconfig is a valid post-condition.

set -euxo pipefail

keys="$(git config --global --name-only --get-regexp \
  '^url\..*x-access-token.*@github\.com/(cockroachlabs/roachmgr|cockroachdb/pebble-private)' \
  2>/dev/null || true)"

if [[ -n "${keys}" ]]; then
  while IFS= read -r key; do
    git config --global --unset-all "${key}" || true
  done <<< "${keys}"
fi
