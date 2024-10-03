#!/bin/bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

exec bazel run -- @io_bazel_rules_go//go/tools/gopackagesdriver "${@}"
