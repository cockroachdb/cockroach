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

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// generateCmd generates the specified files.
var generateCmd = &cobra.Command{
	Use:     "generate [target..]",
	Aliases: []string{"gen"},
	Short:   `Generate the specified files`,
	Long:    `Generate the specified files.`,
	Example: `
	dev generate
	dev generate bazel
	dev generate protobuf
	dev generate {exec,opt}gen`,
	Args: cobra.MinimumNArgs(0),
	// TODO(irfansharif): Errors but default just eaten up. Let's wrap these
	// invocations in something that prints out the appropriate error log
	// (especially considering we've SilenceErrors-ed things away).
	RunE: runGenerate,
}

// TODO(irfansharif): Flesh out the remaining targets.
type generator func(ctx context.Context, cmd *cobra.Command) error

var generators = []generator{
	generateBazel,
}

func runGenerate(cmd *cobra.Command, targets []string) error {
	ctx := context.Background()

	if len(targets) == 0 {
		// Generate all targets.
		for _, gen := range generators {
			if err := gen(ctx, cmd); err != nil {
				return err
			}
		}
		return nil
	}

	for _, target := range targets {
		var gen generator
		switch target {
		case "bazel":
			gen = generateBazel
		default:
			return errors.Newf("unrecognized target: %s", target)
		}

		if err := gen(ctx, cmd); err != nil {
			return err
		}
	}

	return nil
}

func generateBazel(ctx context.Context, cmd *cobra.Command) error {
	return execute(ctx, "bazel", "run", "@cockroach//:gazelle", "--color=yes")
}
