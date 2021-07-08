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
	"fmt"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// makeGenerateCmd constructs the subcommand used to generate the specified
// artifacts.
func makeGenerateCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	return &cobra.Command{
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
		RunE: runE,
	}
}

func (d *dev) generate(cmd *cobra.Command, targets []string) error {
	// TODO(irfansharif): Flesh out the remaining targets.
	var generatorTargetMapping = map[string]func(cmd *cobra.Command) error{
		"bazel": d.generateBazel,
	}

	if len(targets) == 0 {
		// Collect all the targets.
		for target := range generatorTargetMapping {
			targets = append(targets, target)
		}
	}

	for _, target := range targets {
		generator, ok := generatorTargetMapping[target]
		if !ok {
			return errors.Newf("unrecognized target: %s", target)
		}

		if err := generator(cmd); err != nil {
			return err
		}
	}

	return nil
}

func (d *dev) generateBazel(cmd *cobra.Command) error {
	ctx := cmd.Context()
	var err error
	_, err = d.exec.CommandContext(ctx, "bazel",
		"run", "//:gazelle", "--color=yes", "--", "update-repos",
		"-from_file=go.mod", "-build_file_proto_mode=disable_global",
		"-to_macro=DEPS.bzl%go_deps", "-prune=true",
	)
	if err != nil {
		return err
	}

	cwd, err := d.os.Getwd()
	if err != nil {
		return err
	}

	buffer, err := d.exec.CommandContext(ctx, "bazel",
		"run", "//pkg/cmd/generate-test-suites", "--color=yes",
		"--run_under", fmt.Sprintf("cd %s &&", cwd),
	)
	if err != nil {
		return err
	}

	if err := d.os.WriteFile(path.Join(cwd, "pkg/BUILD.bazel"), buffer); err != nil {
		return err
	}

	_, err = d.exec.CommandContext(ctx, "bazel", "run", "@cockroach//:gazelle", "--color=yes")
	return err
}
