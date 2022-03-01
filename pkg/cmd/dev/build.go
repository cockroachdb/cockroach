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
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/alessio/shellescape"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/spf13/cobra"
)

const (
	crossFlag = "cross"
)

type buildTarget struct {
	fullName string
	kind     string
}

// makeBuildCmd constructs the subcommand used to build the specified binaries.
func makeBuildCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	buildCmd := &cobra.Command{
		Use:   "build <binary>",
		Short: "Build the specified binaries",
		Long: fmt.Sprintf(
			"Build the specified binaries either using their bazel targets or one "+
				"of the following shorthands:\n\t%s",
			strings.Join(allBuildTargets, "\n\t"),
		),
		// TODO(irfansharif): Flesh out the example usage patterns.
		Example: `
	dev build cockroach
	dev build cockroach-{short,oss}
	dev build {opt,exec}gen`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	buildCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	buildCmd.Flags().String(crossFlag, "", `
        Turns on cross-compilation. Builds the binary using the builder image w/ Docker.
        You can optionally set a config, as in --cross=windows.
        Defaults to linux if not specified. The config should be the name of a
        build configuration specified in .bazelrc, minus the "cross" prefix.`)
	buildCmd.Flags().Lookup(crossFlag).NoOptDefVal = "linux"
	addCommonBuildFlags(buildCmd)
	return buildCmd
}

// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.
// TODO(irfansharif): Make sure all the relevant binary targets are defined
// above, and in usage docs.

// buildTargetMapping maintains shorthands that map 1:1 with bazel targets.
var buildTargetMapping = map[string]string{
	"bazel-remote":     bazelRemoteTarget,
	"buildifier":       "@com_github_bazelbuild_buildtools//buildifier:buildifier",
	"buildozer":        "@com_github_bazelbuild_buildtools//buildozer:buildozer",
	"cockroach":        "//pkg/cmd/cockroach:cockroach",
	"cockroach-sql":    "//pkg/cmd/cockroach-sql:cockroach-sql",
	"cockroach-oss":    "//pkg/cmd/cockroach-oss:cockroach-oss",
	"cockroach-short":  "//pkg/cmd/cockroach-short:cockroach-short",
	"crlfmt":           "@com_github_cockroachdb_crlfmt//:crlfmt",
	"dev":              "//pkg/cmd/dev:dev",
	"docgen":           "//pkg/cmd/docgen:docgen",
	"execgen":          "//pkg/sql/colexec/execgen/cmd/execgen:execgen",
	"gofmt":            "@com_github_cockroachdb_gostdlib//cmd/gofmt:gofmt",
	"goimports":        "@com_github_cockroachdb_gostdlib//x/tools/cmd/goimports:goimports",
	"label-merged-pr":  "//pkg/cmd/label-merged-pr:label-merged-pr",
	"optgen":           "//pkg/sql/opt/optgen/cmd/optgen:optgen",
	"optfmt":           "//pkg/sql/opt/optgen/cmd/optfmt:optfmt",
	"oss":              "//pkg/cmd/cockroach-oss:cockroach-oss",
	"langgen":          "//pkg/sql/opt/optgen/cmd/langgen:langgen",
	"roachprod":        "//pkg/cmd/roachprod:roachprod",
	"roachprod-stress": "//pkg/cmd/roachprod-stress:roachprod-stress",
	"roachtest":        "//pkg/cmd/roachtest:roachtest",
	"short":            "//pkg/cmd/cockroach-short:cockroach-short",
	"staticcheck":      "@co_honnef_go_tools//cmd/staticcheck:staticcheck",
	"stress":           stressTarget,
	"workload":         "//pkg/cmd/workload:workload",
}

// allBuildTargets is a sorted list of all the available build targets.
var allBuildTargets = func() []string {
	ret := make([]string, 0, len(buildTargetMapping))
	for t := range buildTargetMapping {
		ret = append(ret, t)
	}
	sort.Strings(ret)
	return ret
}()

func (d *dev) build(cmd *cobra.Command, commandLine []string) error {
	targets, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	cross := mustGetFlagString(cmd, crossFlag)

	args, buildTargets, err := d.getBasicBuildArgs(ctx, targets)
	if err != nil {
		return err
	}
	args = append(args, additionalBazelArgs...)

	if cross == "" {
		// run "yarn --check-files" before any bazel target that includes UI to ensure that node_modules dir is consistent
		// see related issue: https://github.com/cockroachdb/cockroach/issues/70867
		for _, arg := range args {
			if arg == "--config=with_ui" {
				logCommand("bazel", "run", "@nodejs//:yarn", "--", "--check-files", "--cwd", "pkg/ui", "--offline")
				if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", "run", "@nodejs//:yarn", "--", "--check-files", "--cwd", "pkg/ui", "--offline"); err != nil {
					return err
				}
				break
			}
		}
		logCommand("bazel", args...)
		if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
			return err
		}
		return d.stageArtifacts(ctx, buildTargets)
	}
	volume := mustGetFlagString(cmd, volumeFlag)
	cross = "cross" + cross
	return d.crossBuild(ctx, args, buildTargets, cross, volume)
}

func (d *dev) crossBuild(
	ctx context.Context, bazelArgs []string, targets []buildTarget, crossConfig string, volume string,
) error {
	bazelArgs = append(bazelArgs, fmt.Sprintf("--config=%s", crossConfig), "--config=ci")
	configArgs := getConfigArgs(bazelArgs)
	dockerArgs, err := d.getDockerRunArgs(ctx, volume, false)
	if err != nil {
		return err
	}
	// Construct a script that builds the binaries and copies them
	// to the appropriate location in /artifacts.
	var script strings.Builder
	script.WriteString("set -euxo pipefail\n")
	script.WriteString(fmt.Sprintf("bazel %s\n", shellescape.QuoteCommand(bazelArgs)))
	for _, arg := range bazelArgs {
		if arg == "--config=with_ui" {
			script.WriteString("bazel run @nodejs//:yarn -- --check-files --cwd pkg/ui --offline\n")
			break
		}
	}
	var bazelBinSet bool
	script.WriteString("set +x\n")
	for _, target := range targets {
		if target.kind == "cmake" {
			if !bazelBinSet {
				script.WriteString(fmt.Sprintf("BAZELBIN=$(bazel info bazel-bin %s)\n", shellescape.QuoteCommand(configArgs)))
				bazelBinSet = true
			}
			targetComponents := strings.Split(target.fullName, ":")
			pkgname := strings.TrimPrefix(targetComponents[0], "//")
			dirname := targetComponents[1]
			script.WriteString(fmt.Sprintf("cp -R $BAZELBIN/%s/%s /artifacts/%s\n", pkgname, dirname, dirname))
			script.WriteString(fmt.Sprintf("chmod a+w -R /artifacts/%s\n", dirname))
			script.WriteString(fmt.Sprintf("echo \"Successfully built target %s at artifacts/%s\"\n", target.fullName, dirname))
			continue
		}
		// NB: For test targets, the `stdout` output from `bazel run` is
		// going to have some extra garbage. We grep ^/ to select out
		// only the filename we're looking for.
		script.WriteString(fmt.Sprintf("BIN=$(bazel run %s %s --run_under=realpath | grep ^/ | tail -n1)\n", target.fullName, shellescape.QuoteCommand(configArgs)))
		script.WriteString("cp $BIN /artifacts\n")
		script.WriteString("chmod a+w /artifacts/$(basename $BIN)\n")
		script.WriteString(fmt.Sprintf("echo \"Successfully built binary for target %s at artifacts/$(basename $BIN)\"\n", target.fullName))
	}
	_, err = d.exec.CommandContextWithInput(ctx, script.String(), "docker", dockerArgs...)
	return err
}

func (d *dev) stageArtifacts(ctx context.Context, targets []buildTarget) error {
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
		if target.kind != "go_binary" {
			// Skip staging for these.
			continue
		}
		binaryPath := filepath.Join(bazelBin, bazelutil.OutputOfBinaryRule(target.fullName, runtime.GOOS == "windows"))
		base := targetToBinBasename(target.fullName)
		var symlinkPaths []string
		// Binaries beginning with the string "cockroach" go right at
		// the top of the workspace; others go in the `bin` directory.
		if strings.HasPrefix(base, "cockroach") {
			symlinkPaths = append(symlinkPaths, filepath.Join(workspace, base))
			if strings.HasPrefix(base, "cockroach-short") {
				symlinkPaths = append(symlinkPaths, filepath.Join(workspace, "cockroach"))
			}
		} else if base == "dev" {
			buf, err := d.os.ReadFile(filepath.Join(workspace, "dev"))
			if err != nil {
				return err
			}
			var devVersion string
			for _, line := range strings.Split(buf, "\n") {
				if strings.HasPrefix(line, "DEV_VERSION=") {
					devVersion = strings.Trim(strings.TrimPrefix(line, "DEV_VERSION="), "\n ")
				}
			}
			if devVersion == "" {
				return errors.New("could not find DEV_VERSION in top-level `dev` script")
			}

			symlinkPaths = append(symlinkPaths,
				filepath.Join(workspace, "bin", "dev-versions", fmt.Sprintf("dev.%s", devVersion)))
		} else {
			symlinkPaths = append(symlinkPaths, filepath.Join(workspace, "bin", base))
		}

		// Symlink from binaryPath -> symlinkPath, clear out detritus, if any.
		for _, symlinkPath := range symlinkPaths {
			if err := d.os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err := d.os.Symlink(binaryPath, symlinkPath); err != nil {
				return err
			}
			rel, err := filepath.Rel(workspace, symlinkPath)
			if err != nil {
				rel = symlinkPath
			}
			log.Printf("Successfully built binary for target %s at %s", target.fullName, rel)
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
func (d *dev) getBasicBuildArgs(
	ctx context.Context, targets []string,
) (args []string, buildTargets []buildTarget, _ error) {
	if len(targets) == 0 {
		// Default to building the cockroach binary.
		targets = append(targets, "cockroach")
	}

	args = append(args, "build")
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}

	shouldBuildWithTestConfig := false
	for _, target := range targets {
		target = strings.TrimPrefix(target, "./")
		target = strings.TrimRight(target, "/")
		// For targets beginning with // or containing / or :, we need to ask Bazel
		// what kind of target it is.
		if strings.HasPrefix(target, "//") || strings.ContainsAny(target, "/:") {
			queryArgs := []string{"query", target, "--output=label_kind"}
			labelKind, queryErr := d.exec.CommandContextSilent(ctx, "bazel", queryArgs...)
			if queryErr != nil {
				return nil, nil, fmt.Errorf("could not run `bazel %s` (%w)",
					shellescape.QuoteCommand(queryArgs), queryErr)
			}
			for _, line := range strings.Split(strings.TrimSpace(string(labelKind)), "\n") {
				fields := strings.Fields(line)
				fullTargetName := fields[len(fields)-1]
				typ := fields[0]
				args = append(args, fullTargetName)
				buildTargets = append(buildTargets, buildTarget{fullName: fullTargetName, kind: typ})
				if typ == "go_test" || typ == "go_transition_test" {
					shouldBuildWithTestConfig = true
				}
			}
			continue
		}

		aliased, ok := buildTargetMapping[target]
		if !ok {
			return nil, nil, fmt.Errorf("unrecognized target: %s", target)
		}

		args = append(args, aliased)
		buildTargets = append(buildTargets, buildTarget{fullName: aliased, kind: "go_binary"})
	}

	// Add --config=with_ui iff we're building a target that needs it.
	for _, target := range buildTargets {
		if target.fullName == buildTargetMapping["cockroach"] || target.fullName == buildTargetMapping["cockroach-oss"] {
			args = append(args, "--config=with_ui")
			break
		}
	}
	if shouldBuildWithTestConfig {
		args = append(args, "--config=test")
	}
	return args, buildTargets, nil
}

// Given a list of Bazel arguments, find the ones starting with --config= and
// return them.
func getConfigArgs(args []string) (ret []string) {
	for _, arg := range args {
		if strings.HasPrefix(arg, "--config=") {
			ret = append(ret, arg)
		}
	}
	return
}
