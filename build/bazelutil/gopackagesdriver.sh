#!/bin/bash
exec bazel run -- @io_bazel_rules_go//go/tools/gopackagesdriver "${@}"
