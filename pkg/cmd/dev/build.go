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
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// buildCmd builds the specified binaries.
var buildCmd = &cobra.Command{
	Use:   "build <binary>",
	Short: "Build the specified binaries",
	Long:  "Build the specified binaries.",
	// TODO(irfansharif): Flesh out the example usage patterns.
	Example: `
	dev build cockroach --tags=deadlock
	dev build cockroach-{short,oss}
	dev build {opt,exec}gen`,
	Args: cobra.MinimumNArgs(0),
	RunE: runBuild,
}

var buildTargetMapping = map[string]string{
	"cockroach":        "@cockroach//pkg/cmd/cockroach",
	"cockroach-oss":    "@cockroach//pkg/cmd/cockroach-oss",
	"cockroach-short":  "@cockroach//pkg/cmd/cockroach-short",
	"dev":              "@cockroach//pkg/cmd/dev",
	"docgen":           "@cockroach//pkg/cmd/docgen",
	"execgen":          "@cockroach//pkg/sql/colexec/execgen/cmd/execgen",
	"optgen":           "@cockroach//pkg/sql/opt/optgen/cmd/optgen",
	"optfmt":           "@cockroach//pkg/sql/opt/optgen/cmd/optfmt",
	"langgen":          "@cockroach//pkg/sql/opt/optgen/cmd/langgen",
	"roachprod":        "@cockroach//pkg/cmd/roachprod",
	"roachprod-stress": "@cockroach//pkg/cmd/roachprod-stress",
	"workload":         "@cockroach//pkg/cmd/workload",
	"roachtest":        "@cockroach//pkg/cmd/roachtest",
}

func runBuild(cmd *cobra.Command, targets []string) error {
	ctx := context.Background()

	if len(targets) == 0 {
		// Default to building the cockroach binary.
		targets = append(targets, "cockroach")
	}

	// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.
	// TODO(irfansharif): Extract built binaries out of the bazel sandbox.
	// TODO(irfansharif): Make sure all the relevant binary targets are defined
	// above, and in usage docs.

	var args []string
	args = append(args, "build")
	args = append(args, "--color=yes")

	for _, target := range targets {
		buildTarget, ok := buildTargetMapping[target]
		if !ok {
			log.Errorf(ctx, "unrecognized target: %s", target)
			return errors.Newf("unrecognized target")
		}

		args = append(args, buildTarget)
	}
	return execute(ctx, "bazel", args...)
}
