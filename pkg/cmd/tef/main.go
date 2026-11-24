// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package main provides the entry point for the Task Execution Framework (TEF) CLI.
// This binary initializes and executes the TEF command-line interface, which provides
// commands for managing workflow execution, starting workers, and interacting with plans.
package main

import "github.com/cockroachdb/cockroach/pkg/cmd/tef/cli"

// main is the entry point for the TEF application.
// It initializes and executes the CLI, which provides commands for managing workflow execution.
func main() {
	cli.Initialize()
}
