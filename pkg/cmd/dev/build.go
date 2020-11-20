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

// buildCmd builds the specified binaries.
var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build the specified binaries",
	Long:  "Build the specified binaries.",
	Example: `
	dev build cockroach --tags=deadlock
	dev build cockroach-{short,oss}
	dev build {opt,exec}gen`,
	Args: cobra.NoArgs,
	RunE: runBuild,
}

func runBuild(cmd *cobra.Command, args []string) error {
	// TODO(irfansharif): Flesh out the example usage patterns.
	return errors.New("unimplemented")
}
