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

// testCmd runs the specified cockroachdb tests.
var testCmd = &cobra.Command{
	Use:   "test [pkg] (flags)",
	Short: `Run the specified tests`,
	Long:  `Run the specified tests.`,
	Example: `
	dev test kv/kvserver --filter=TestReplicaGC* -v -show-logs --timeout=1m
	dev test --stress --race ...
	dev test --logic --files=prepare|fk --subtests=20042 --config=local
	dev test --fuzz sql/sem/tree --filter=Decimal`,
	Args: cobra.NoArgs,
	RunE: runTest,
}

func runTest(cmd *cobra.Command, args []string) error {
	// TODO(irfansharif): Flesh out the example usage patterns.
	return errors.New("unimplemented")
}
