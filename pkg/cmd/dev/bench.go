// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// benchCmd runs the specified cockroachdb benchmarks.
var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: `Run the specified benchmarks`,
	Long:  `Run the specified benchmarks.`,
	Example: `
	dev bench --pkg=sql/parser --filter=BenchmarkParse`,
	Args: cobra.NoArgs,
	RunE: runBench,
}

func runBench(cmd *cobra.Command, args []string) error {
	// TODO(irfansharif): Flesh out the example usage patterns.
	return errors.New("unimplemented")
}
