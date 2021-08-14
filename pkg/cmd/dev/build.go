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
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/spf13/cobra"
)

const (
	crossFlag              = "cross"
	hoistGeneratedCodeFlag = "hoist-generated-code"
)

// makeBuildCmd constructs the subcommand used to build the specified binaries.
func makeBuildCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	buildCmd := &cobra.Command{
		Use:   "build <binary>",
		Short: "Build the specified binaries",
		Long:  "Build the specified binaries.",
		// TODO(irfansharif): Flesh out the example usage patterns.
		Example: `
	dev build cockroach
	dev build cockroach-{short,oss}
	dev build {opt,exec}gen`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	buildCmd.Flags().String(volumeFlag, "bzlcache", "the Docker volume to use as the Bazel cache (only used for cross builds)")
	buildCmd.Flags().String(crossFlag, "", `
        Turns on cross-compilation. Builds the binary using the builder image w/ Docker.
        You can optionally set a config, as in --cross=windows.
        Defaults to linux if not specified. The config should be the name of a
        build configuration specified in .bazelrc, minus the "cross" prefix.`)
	buildCmd.Flags().Bool(hoistGeneratedCodeFlag, false, "hoist generated code out of the Bazel sandbox into the workspace")
	buildCmd.Flags().Lookup(crossFlag).NoOptDefVal = "linux"
	return buildCmd
}

// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.
// TODO(irfansharif): Make sure all the relevant binary targets are defined
// above, and in usage docs.

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
	"short":            "//pkg/cmd/cockroach-short",
	"workload":         "//pkg/cmd/workload",
	"roachtest":        "//pkg/cmd/roachtest",
}

func (d *dev) build(cmd *cobra.Command, targets []string) error {
	ctx := cmd.Context()
	cross := mustGetFlagString(cmd, crossFlag)
	hoistGeneratedCode := mustGetFlagBool(cmd, hoistGeneratedCodeFlag)

	args, fullTargets, err := getBasicBuildArgs(targets)
	if err != nil {
		return err
	}

	if cross == "" {
		args = append(args, getConfigFlags()...)
		if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
			return err
		}
		return d.stageArtifacts(ctx, fullTargets, hoistGeneratedCode)
	}
	// Cross-compilation case.
	cross = "cross" + cross
	volume := mustGetFlagString(cmd, volumeFlag)
	args = append(args, fmt.Sprintf("--config=%s", cross))
	dockerArgs, err := d.getDockerRunArgs(ctx, volume, false)
	if err != nil {
		return err
	}
	// Construct a script that builds the binaries and copies them
	// to the appropriate location in /artifacts.
	var script strings.Builder
	script.WriteString("set -euxo pipefail\n")
	// TODO(ricky): Actually, we need to shell-quote the arguments,
	// but that's hard and I don't think it's necessary for now.
	script.WriteString(fmt.Sprintf("bazel %s\n", strings.Join(args, " ")))
	script.WriteString(fmt.Sprintf("BAZELBIN=`bazel info bazel-bin --color=no --config=%s`\n", cross))
	for _, target := range fullTargets {
		script.WriteString(fmt.Sprintf("cp $BAZELBIN/%s /artifacts\n", bazelutil.OutputOfBinaryRule(target)))
		script.WriteString(fmt.Sprintf("chmod +w /artifacts/%s\n", targetToBinBasename(target)))
	}
	_, err = d.exec.CommandContextWithInput(ctx, script.String(), "docker", dockerArgs...)
	if err != nil {
		return err
	}
	for _, target := range fullTargets {
		logSuccessfulBuild(target, filepath.Join("artifacts", targetToBinBasename(target)))
	}
	return nil
}

func (d *dev) stageArtifacts(ctx context.Context, targets []string, hoistGeneratedCode bool) error {
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	// Create the bin directory.
	if err = d.os.MkdirAll(path.Join(workspace, "bin")); err != nil {
		return err
	}
	bazelBin, err := d.getBazelBin(ctx)
	if err != nil {
		return err
	}

	for _, target := range targets {
		binaryPath := filepath.Join(bazelBin, bazelutil.OutputOfBinaryRule(target))
		base := targetToBinBasename(target)
		var symlinkPath string
		// Binaries beginning with the string "cockroach" go right at
		// the top of the workspace; others go in the `bin` directory.
		if strings.HasPrefix(base, "cockroach") {
			symlinkPath = filepath.Join(workspace, base)
		} else {
			symlinkPath = filepath.Join(workspace, "bin", base)
		}

		// Symlink from binaryPath -> symlinkPath
		if err := d.os.Remove(symlinkPath); err != nil && !oserror.IsNotExist(err) {
			return err
		}
		if err := d.os.Symlink(binaryPath, symlinkPath); err != nil {
			return err
		}
		rel, err := filepath.Rel(workspace, symlinkPath)
		if err != nil {
			rel = symlinkPath
		}
		logSuccessfulBuild(target, rel)
	}

	if hoistGeneratedCode {
		goFiles, err := d.os.ListFilesWithSuffix(filepath.Join(bazelBin, "pkg"), ".go")
		if err != nil {
			return err
		}
		fileContents, err := d.os.ReadFile(filepath.Join(workspace, "build/bazelutil/checked_in_genfiles.txt"))
		if err != nil {
			return err
		}
		renameMap := make(map[string]string)
		for _, line := range strings.Split(fileContents, "\n") {
			line = strings.TrimSpace(line)
			if len(line) == 0 || strings.HasPrefix(line, "#") {
				continue
			}
			components := strings.Split(line, "|")
			target := components[0]
			dir := strings.Split(strings.TrimPrefix(target, "//"), ":")[0]
			oldBasename := components[1]
			newBasename := components[2]
			renameMap[filepath.Join(dir, oldBasename)] = filepath.Join(dir, newBasename)
		}

		for _, file := range goFiles {
			// We definitely don't want any code that was put in the sandbox for gomock.
			if strings.Contains(file, "_gomock_gopath") {
				continue
			}
			// First case: generated Go code that's checked into tree.
			relPath := strings.TrimPrefix(file, bazelBin+"/")
			dst, ok := renameMap[relPath]
			if ok {
				err := d.os.CopyFile(file, filepath.Join(workspace, dst))
				if err != nil {
					return err
				}
				continue
			}
			// Second case: the pathname contains github.com/cockroachdb/cockroach.
			const cockroachURL = "github.com/cockroachdb/cockroach/"
			ind := strings.LastIndex(file, cockroachURL)
			if ind > 0 {
				// If the cockroach URL was found in the filepath, then we should
				// trim everything up to and including the URL to find the path
				// where the file should be staged.
				loc := file[ind+len(cockroachURL):]
				err := d.os.CopyFile(file, filepath.Join(workspace, loc))
				if err != nil {
					return err
				}
				continue
			}
			// Third case: apply a heuristic to see whether this file makes sense to be
			// staged.
			pathComponents := strings.Split(file, string(os.PathSeparator))
			var skip bool
			for _, component := range pathComponents[:len(pathComponents)-1] {
				// Pretty decent heuristic for whether a file needs to be staged.
				// When path components contain ., they normally are generated files
				// from third-party packages, as in google.golang.org. Meanwhile,
				// when path components end in _, that usually represents internal
				// stuff that doesn't need to be staged, like
				// pkg/cmd/dev/dev_test_/testmain.go. Note that generated proto code
				// is handled by the cockroach URL case above.
				if len(component) > 0 && (strings.ContainsRune(component, '.') || component[len(component)-1] == '_') {
					skip = true
				}
			}
			if !skip {
				// Failures here don't mean much. Just ignore them.
				_ = d.os.CopyFile(file, filepath.Join(workspace, relPath))
			}
		}
	}

	return nil
}

func targetToBinBasename(target string) string {
	base := filepath.Base(strings.TrimPrefix(target, "//"))
	// If there's a colon, the actual name of the executable is
	// after it.
	colon := strings.LastIndex(base, ":")
	if colon >= 0 {
		base = base[colon+1:]
	}
	return base
}

// getBasicBuildArgs is for enumerating the arguments to pass to `bazel` in
// order to build the given high-level targets.
// The first string slice returned is the list of arguments (i.e. to pass to
// `CommandContext`), and the second is the full list of targets to be built
// (e.g. after translation, so short -> "//pkg/cmd/cockroach-short").
func getBasicBuildArgs(targets []string) (args, fullTargets []string, err error) {
	if len(targets) == 0 {
		// Default to building the cockroach binary.
		targets = append(targets, "cockroach")
	}

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
		target = strings.TrimRight(target, "/")
		// Assume that targets beginning with `//` or containing `/`
		// don't need to be munged.
		if strings.HasPrefix(target, "//") || strings.Contains(target, "/") {
			args = append(args, target)
			fullTargets = append(fullTargets, target)
			continue
		}
		buildTarget, ok := buildTargetMapping[target]
		if !ok {
			err = errors.Newf("unrecognized target: %s", target)
			return
		}

		fullTargets = append(fullTargets, buildTarget)
		args = append(args, buildTarget)
	}
	return
}

func logSuccessfulBuild(target, rel string) {
	log.Printf("Successfully built binary for target %s at %s", target, rel)
}
