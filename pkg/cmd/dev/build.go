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
	"path"
	"strings"

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
			log.Errorf(ctx, "unrecognized target: %s", target)
			return errors.Newf("unrecognized target")
		}

		args = append(args, buildTarget)
	}
	if err := execute(ctx, "bazel", args...); err != nil {
		return err
	}

	workspaceBuf, err := executeReturningStdout(ctx, "bazel", "info", "workspace")
	if err != nil {
		return err
	}
	workspace := strings.TrimSpace(workspaceBuf.String())

	var binDirCreated bool
	for _, target := range targets {
		buildTarget := buildTargetMapping[target]
		output, err := getPathToGoBin(ctx, buildTarget)
		if err != nil {
			return err
		}
		
		base := path.Base(output)
		var dest string
		// Binaries beginning with the string "cockroach" go right at
		// the top of the workspace; others go in the `bin` directory.
		if strings.HasPrefix(base, "cockroach") {
			dest = path.Join(workspace, base)
		} else {
			if !binDirCreated {
				if err := os.MkdirAll(path.Join(workspace, "bin"), 0755); err != nil {
					return err
				}
				binDirCreated = true
			}
			dest = path.Join(workspace, "bin", base)
		}

		// Symlink from output -> dest
		if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Symlink(output, dest); err != nil {
			return err
		}
	}

	return nil
}
