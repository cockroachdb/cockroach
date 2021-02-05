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
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
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
	"cockroach":        "//pkg/cmd/cockroach",
	"cockroach-oss":    "//pkg/cmd/cockroach-oss",
	"cockroach-short":  "//pkg/cmd/cockroach-short",
	"dev":              "//pkg/cmd/dev",
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

func runBuild(cmd *cobra.Command, targets []string) error {
	ctx := context.Background()

	if len(targets) == 0 {
		// Default to building the cockroach binary.
		targets = append(targets, "cockroach")
	}

	// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.
	// TODO(irfansharif): Make sure all the relevant binary targets are defined
	// above, and in usage docs.

	var args []string
	args = append(args, "build")
	args = append(args, "--color=yes")

	for _, target := range targets {
		buildTarget, ok := buildTargetMapping[target]
		if !ok {
			return errors.Newf("unrecognized target: %s", target)
		}

		args = append(args, buildTarget)
	}
	if err := execute(ctx, "bazel", args...); err != nil {
		return err
	}
	return symlinkBinaries(targets)
}

func symlinkBinaries(targets []string) error {
	var workspace string
	{
		out, err := exec.Command("bazel", "info", "workspace").Output()
		if err != nil {
			return err
		}
		workspace = strings.TrimSpace(string(out))
	}

	// Create the bin directory.
	if err := os.MkdirAll(path.Join(workspace, "bin"), 0755); err != nil {
		return err
	}

	for _, target := range targets {
		buildTarget := buildTargetMapping[target]
		binaryPath, err := getPathToBin(buildTarget)
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
		if err := os.Remove(symlinkPath); err != nil && !oserror.IsNotExist(err) {
			return err
		}
		if err := os.Symlink(binaryPath, symlinkPath); err != nil {
			return err
		}
	}

	return nil
}
