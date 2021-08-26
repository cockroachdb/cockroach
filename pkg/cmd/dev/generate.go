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
	"path/filepath"
	"strings"

	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
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
        dev generate docs
        dev generate go
`,
		Args: cobra.MinimumNArgs(0),
		// TODO(irfansharif): Errors but default just eaten up. Let's wrap these
		// invocations in something that prints out the appropriate error log
		// (especially considering we've SilenceErrors-ed things away).
		RunE: runE,
	}
}

func (d *dev) generate(cmd *cobra.Command, targets []string) error {
	var generatorTargetMapping = map[string]func(cmd *cobra.Command) error{
		"bazel":   d.generateBazel,
		"docs":    d.generateDocs,
		"execgen": d.generateUnimplemented,
		"go":      d.generateGo,
		"optgen":  d.generateUnimplemented,
		"proto":   d.generateUnimplemented,
	}

	if len(targets) == 0 {
		// Default: generate everything.
		// TODO(ricky): This could be implemented more efficiently --
		// `generate docs` and `generate go` re-do some of the same
		// work and call into Bazel more often than necessary. Fix that
		// when people start to complain.
		targets = append(targets, "bazel", "docs", "go")
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
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	return d.exec.CommandContextInheritingStdStreams(ctx, filepath.Join(workspace, "build", "bazelutil", "bazel-generate.sh"))
}

func (d *dev) generateDocs(cmd *cobra.Command) error {
	ctx := cmd.Context()
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	// List targets we need to build.
	targetsFile, err := d.os.ReadFile(filepath.Join(workspace, "docs/generated/bazel_targets.txt"))
	if err != nil {
		return err
	}
	var targets []string
	for _, line := range strings.Split(targetsFile, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "//") {
			targets = append(targets, line)
		}
	}
	// Build targets.
	var args []string
	args = append(args, "build", "--color=yes", "--experimental_convenience_symlinks=ignore")
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	args = append(args, getConfigFlags()...)
	args = append(args, targets...)
	err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	if err != nil {
		return err
	}
	// Copy docs from bazel-bin to workspace.
	bazelBin, err := d.getBazelBin(ctx)
	if err != nil {
		return err
	}
	for _, target := range targets {
		query, err := d.exec.CommandContextSilent(ctx, "bazel", "query", "--output=xml", target)
		if err != nil {
			return err
		}
		outputs, err := bazelutil.OutputsOfGenrule(target, string(query))
		if err != nil {
			return err
		}
		for _, output := range outputs {
			err = d.os.CopyFile(filepath.Join(bazelBin, output), filepath.Join(workspace, output))
			if err != nil {
				return err
			}
		}
	}
	// docs/generated/redact_safe.md needs special handling.
	output, err := d.exec.CommandContextSilent(ctx, filepath.Join(workspace, "build", "bazelutil", "generate_redact_safe.sh"))
	if err != nil {
		return err
	}
	return d.os.WriteFile(filepath.Join(workspace, "docs", "generated", "redact_safe.md"), string(output))
}

func (d *dev) generateGo(cmd *cobra.Command) error {
	ctx := cmd.Context()
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	// List targets we need to build.
	contents, err := d.os.ReadFile(filepath.Join(workspace, "build/bazelutil/checked_in_genfiles.txt"))
	if err != nil {
		return err
	}
	var lines []string
	for _, line := range strings.Split(contents, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	var targets []string
	for _, line := range lines {
		targets = append(targets, strings.Split(line, "|")[0])
	}
	// Build targets.
	var args []string
	args = append(args, "build", "--color=yes", "--experimental_convenience_symlinks=ignore")
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	args = append(args, getConfigFlags()...)
	args = append(args, targets...)
	err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	if err != nil {
		return err
	}
	// Copy from bazel-bin to workspace.
	bazelBin, err := d.getBazelBin(ctx)
	if err != nil {
		return err
	}
	for _, line := range lines {
		components := strings.Split(line, "|")
		target := components[0]
		dir := strings.Split(strings.TrimPrefix(target, "//"), ":")[0]
		oldBasename := components[1]
		newBasename := components[2]
		err = d.os.CopyFile(filepath.Join(bazelBin, dir, oldBasename),
			filepath.Join(workspace, dir, newBasename))
		if err != nil {
			return err
		}
	}
	return nil
}

func (*dev) generateUnimplemented(*cobra.Command) error {
	return errors.New("To hoist all generated code into the workspace, run " +
		"`dev build` with the flag `--hoist-generated-code`; to build the generated Go " +
		"code needed to pass CI, run `dev generate go`")
}
