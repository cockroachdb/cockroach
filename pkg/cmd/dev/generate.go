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
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const mirrorFlag = "mirror"
const forceFlag = "force"

// makeGenerateCmd constructs the subcommand used to generate the specified
// artifacts.
func makeGenerateCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	generateCmd := &cobra.Command{
		Use:     "generate [target..]",
		Aliases: []string{"gen"},
		Short:   `Generate the specified files`,
		Long:    `Generate the specified files.`,
		Example: `
        dev generate
        dev generate bazel
        dev generate docs
        dev generate go
        dev generate protobuf
        dev generate go+docs
`,
		Args: cobra.MinimumNArgs(0),
		// TODO(irfansharif): Errors but default just eaten up. Let's wrap these
		// invocations in something that prints out the appropriate error log
		// (especially considering we've SilenceErrors-ed things away).
		RunE: runE,
	}
	generateCmd.Flags().Bool(mirrorFlag, false, "mirror new dependencies to cloud storage")
	generateCmd.Flags().Bool(forceFlag, false, "force regeneration even if relevant files are unchanged from upstream")
	return generateCmd
}

func (d *dev) generate(cmd *cobra.Command, targets []string) error {
	var generatorTargetMapping = map[string]func(cmd *cobra.Command) error{
		"bazel":    d.generateBazel,
		"docs":     d.generateDocs,
		"go":       d.generateGo,
		"protobuf": d.generateProtobuf,
		"go+docs":  d.generateGoAndDocs,
	}

	if len(targets) == 0 {
		targets = append(targets, "bazel", "go+docs")
	}

	for _, target := range targets {
		generator, ok := generatorTargetMapping[target]
		if !ok {
			return fmt.Errorf("unrecognized target: %s", target)
		}
		if err := generator(cmd); err != nil {
			return err
		}
	}

	return nil
}

func (d *dev) generateBazel(cmd *cobra.Command) error {
	ctx := cmd.Context()
	mirror := mustGetFlagBool(cmd, mirrorFlag)
	force := mustGetFlagBool(cmd, forceFlag)
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	executable := filepath.Join(workspace, "build", "bazelutil", "bazel-generate.sh")
	env := os.Environ()
	if mirror {
		envvar := "COCKROACH_BAZEL_CAN_MIRROR=1"
		d.log.Printf("export %s", envvar)
		env = append(env, envvar)
	}
	if force {
		envvar := "COCKROACH_BAZEL_FORCE_GENERATE=1"
		d.log.Printf("export %s", envvar)
		env = append(env, envvar)
	}
	return d.exec.CommandContextWithEnv(ctx, env, executable)
}

func (d *dev) generateDocs(cmd *cobra.Command) error {
	ctx := cmd.Context()
	if err := d.generateTarget(ctx, "//pkg/gen:docs"); err != nil {
		return err
	}
	return d.generateRedactSafe(ctx)
}

func (d *dev) generateGoAndDocs(cmd *cobra.Command) error {
	ctx := cmd.Context()
	if err := d.generateTarget(ctx, "//pkg/gen"); err != nil {
		return err
	}
	return d.generateRedactSafe(ctx)
}

func (d *dev) generateGo(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:code")
}

func (d *dev) generateProtobuf(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:go_proto")
}

func (d *dev) generateTarget(ctx context.Context, target string) error {
	if err := d.exec.CommandContextInheritingStdStreams(
		ctx, "bazel", "run", target,
	); err != nil {
		// nolint:errwrap
		return fmt.Errorf("generating target %s: %s", target, err.Error())
	}
	return nil
}

func (d *dev) generateRedactSafe(ctx context.Context) error {
	// docs/generated/redact_safe.md needs special handling.
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	output, err := d.exec.CommandContextSilent(
		ctx, filepath.Join(workspace, "build", "bazelutil", "generate_redact_safe.sh"),
	)
	if err != nil {
		// nolint:errwrap
		return fmt.Errorf("generating redact_safe.md: %s", err.Error())
	}
	return d.os.WriteFile(
		filepath.Join(workspace, "docs", "generated", "redact_safe.md"), string(output),
	)
}
