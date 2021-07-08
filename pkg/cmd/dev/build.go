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
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/spf13/cobra"
)

// makeBuildCmd constructs the subcommand used to build the specified binaries.
func makeBuildCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	return &cobra.Command{
		Use:   "build <binary>",
		Short: "Build the specified binaries",
		Long:  "Build the specified binaries.",
		// TODO(irfansharif): Flesh out the example usage patterns.
		Example: `
	dev build cockroach --tags=deadlock
	dev build cockroach-{short,oss}
	dev build {opt,exec}gen`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
}

// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.
// TODO(irfansharif): Make sure all the relevant binary targets are defined
// above, and in usage docs.

var buildTargetMapping = map[string]string{
	"cockroach":        "//pkg/cmd/cockroach",
	"cockroach-oss":    "//pkg/cmd/cockroach-oss",
	"cockroach-short":  "//pkg/cmd/cockroach-short",
	"docgen":           "//pkg/cmd/docgen",
	"execgen":          "//pkg/sql/colexec/execgen/cmd/execgen",
	"optgen":           "//pkg/sql/opt/optgen/cmd/optgen",
	"optfmt":           "//pkg/sql/opt/optgen/cmd/optfmt",
	"langgen":          "//pkg/sql/opt/optgen/cmd/langgen",
	"roachprod":        "//pkg/cmd/roachprod",
	"roachprod-stress": "//pkg/cmd/roachprod-stress",
	"workload":         "//pkg/cmd/workload",
	"roachtest":        "//pkg/cmd/roachtest",
}

func (d *dev) build(cmd *cobra.Command, targets []string) (err error) {
	ctx := cmd.Context()

	if len(targets) == 0 {
		// Default to building the cockroach binary.
		targets = append(targets, "cockroach")
	}

	var args []string
	args = append(args, "build")
	args = append(args, "--color=yes")
	// Don't let bazel generate any convenience symlinks, we'll create them
	// ourself.
	args = append(args, "--experimental_convenience_symlinks=ignore")
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}

	for _, target := range targets {
		buildTarget, ok := buildTargetMapping[target]
		if !ok {
			return errors.Newf("unrecognized target: %s", target)
		}

		args = append(args, buildTarget)
	}

	if _, err := d.exec.CommandContext(ctx, "bazel", args...); err != nil {
		return err
	}

	return d.symlinkBinaries(ctx, targets)
}

func (d *dev) symlinkBinaries(ctx context.Context, targets []string) error {
	var workspace string
	{
		out, err := d.exec.CommandContextSilent(ctx, "bazel", "info", "workspace", "--color=yes")
		if err != nil {
			return err
		}
		workspace = strings.TrimSpace(string(out))
	}

	// Create the bin directory.
	if err := d.os.MkdirAll(path.Join(workspace, "bin")); err != nil {
		return err
	}

	for _, target := range targets {
		buildTarget := buildTargetMapping[target]
		binaryPath, err := d.getPathToBin(ctx, buildTarget)
		if err != nil {
			return err
		}

		var symlinkPath string
		// Binaries beginning with the string "cockroach" go right at
		// the top of the workspace; others go in the `bin` directory.
		if strings.HasPrefix(target, "cockroach") {
			symlinkPath = path.Join(workspace, target)
		} else {
			symlinkPath = path.Join(workspace, "bin", target)
		}

		// Symlink from binaryPath -> symlinkPath
		if err := d.os.Remove(symlinkPath); err != nil && !oserror.IsNotExist(err) {
			return err
		}
		if err := d.os.Symlink(binaryPath, symlinkPath); err != nil {
			return err
		}
	}

	return nil
}

func (d *dev) getPathToBin(ctx context.Context, target string) (string, error) {
	// actionQueryResult is used to unmarshal the results of the bazel action
	// query.
	type actionQueryResult struct {
		Artifacts []struct {
			ID       string `json:"id"`
			ExecPath string `json:"execPath"`
		} `json:"artifacts"`

		Actions []struct {
			Mnemonic  string   `json:"mnemonic"`
			OutputIds []string `json:"outputIds"`
		} `json:"actions"`
	}

	buf, err := d.exec.CommandContextSilent(ctx, "bazel", "aquery", target, "--output=jsonproto", "--color=yes")
	if err != nil {
		return "", err
	}

	var result actionQueryResult
	if err := json.Unmarshal(buf, &result); err != nil {
		return "", err
	}

	const binaryMnemomic = "GoLink"
	for _, action := range result.Actions {
		if action.Mnemonic == binaryMnemomic {
			id := action.OutputIds[0]
			for _, artifact := range result.Artifacts {
				if artifact.ID == id {
					binaryPath := strings.TrimPrefix(artifact.ExecPath, "bazel-out/")
					var outputPath string
					{
						out, err := d.exec.CommandContextSilent(ctx, "bazel", "info", "output_path", "--color=yes")
						if err != nil {
							return "", err
						}
						outputPath = strings.TrimSpace(string(out))
					}
					// This extra wrangling here is to avoid symlinking through `bazel-out`,
					// given we avoid the convenience symlinks up above.
					binaryPath = fmt.Sprintf("%s/%s", outputPath, binaryPath)
					return binaryPath, nil
				}
			}
		}
	}

	return "", errors.Newf("could not find path to binary %q", target)
}
