// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/spf13/cobra"
)

const (
	crossFlag       = "cross"
	lintFlag        = "lint"
	cockroachTarget = "//pkg/cmd/cockroach:cockroach"
	nogoEnableFlag  = "--run_validations"
	nogoDisableFlag = "--norun_validations"
	geosTarget      = "//c-deps:libgeos"
	devTarget       = "//pkg/cmd/dev:dev"
)

type buildTarget struct {
	// fullName is the full qualified name of the Bazel build target,
	// like //pkg/cmd/cockroach:cockroach.
	fullName string
	// kind is either the "kind" of the Bazel rule (like go_library), or the
	// string "geos" in case fullName is //c-deps:libgeos.
	kind string
}

// makeBuildCmd constructs the subcommand used to build the specified binaries.
func makeBuildCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	buildCmd := &cobra.Command{
		Use:   "build <binary>",
		Short: "Build the specified binaries",
		Long: fmt.Sprintf(
			"Build the specified binaries either using their bazel targets or one "+
				"of the following shorthands:\n\n\t%s",
			strings.Join(allBuildTargets, "\n\t"),
		),
		// TODO(irfansharif): Flesh out the example usage patterns.
		Example: `
	dev build cockroach
	dev build cockroach-short
	dev build {opt,exec}gen`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	buildCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	buildCmd.Flags().BoolP(lintFlag, "l", false, "perform linting (nogo) as part of the build process (i.e. override nolintonbuild config)")
	buildCmd.Flags().String(crossFlag, "", "cross-compiles using the builder image (options: linux, linuxarm, macos, macosarm, windows)")
	buildCmd.Flags().Lookup(crossFlag).NoOptDefVal = "linux"
	buildCmd.Flags().StringArray(dockerArgsFlag, []string{}, "additional arguments to pass to Docker (only used for cross builds)")
	addCommonBuildFlags(buildCmd)
	return buildCmd
}

// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.

// buildTargetMapping maintains shorthands that map 1:1 with bazel targets.
var buildTargetMapping = map[string]string{
	"bazel-remote":         bazelRemoteTarget,
	"buildifier":           "@com_github_bazelbuild_buildtools//buildifier:buildifier",
	"buildozer":            "@com_github_bazelbuild_buildtools//buildozer:buildozer",
	"cloudupload":          "//pkg/cmd/cloudupload:cloudupload",
	"cockroach":            cockroachTarget,
	"cockroach-sql":        "//pkg/cmd/cockroach-sql:cockroach-sql",
	"cockroach-short":      "//pkg/cmd/cockroach-short:cockroach-short",
	"crlfmt":               "@com_github_cockroachdb_crlfmt//:crlfmt",
	"dev":                  devTarget,
	"docgen":               "//pkg/cmd/docgen:docgen",
	"docs-issue-gen":       "//pkg/cmd/docs-issue-generation:docs-issue-generation",
	"drt-run":              "//pkg/cmd/drt-run:drt-run",
	"execgen":              "//pkg/sql/colexec/execgen/cmd/execgen:execgen",
	"gofmt":                "@com_github_cockroachdb_gostdlib//cmd/gofmt:gofmt",
	"goimports":            "@com_github_cockroachdb_gostdlib//x/tools/cmd/goimports:goimports",
	"label-merged-pr":      "//pkg/cmd/label-merged-pr:label-merged-pr",
	"geos":                 geosTarget,
	"langgen":              "//pkg/sql/opt/optgen/cmd/langgen:langgen",
	"libgeos":              geosTarget,
	"microbench-ci":        "//pkg/cmd/microbench-ci:microbench-ci",
	"optgen":               "//pkg/sql/opt/optgen/cmd/optgen:optgen",
	"optfmt":               "//pkg/sql/opt/optgen/cmd/optfmt:optfmt",
	"reduce":               "//pkg/cmd/reduce:reduce",
	"drtprod":              "//pkg/cmd/drtprod:drtprod",
	"roachprod":            "//pkg/cmd/roachprod:roachprod",
	"roachprod-stress":     "//pkg/cmd/roachprod-stress:roachprod-stress",
	"roachprod-microbench": "//pkg/cmd/roachprod-microbench:roachprod-microbench",
	"roachtest":            "//pkg/cmd/roachtest:roachtest",
	"short":                "//pkg/cmd/cockroach-short:cockroach-short",
	"smith":                "//pkg/cmd/smith:smith",
	"smithcmp":             "//pkg/cmd/smithcmp:smithcmp",
	"smithtest":            "//pkg/cmd/smithtest:smithtest",
	"sql-bootstrap-data":   "//pkg/cmd/sql-bootstrap-data:sql-bootstrap-data",
	"staticcheck":          "@co_honnef_go_tools//cmd/staticcheck:staticcheck",
	"tests":                "//pkg:all_tests",
	"workload":             "//pkg/cmd/workload:workload",
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
	var tmpDir string
	if !buildutil.CrdbTestBuild {
		// tmpDir will contain the build event binary file if produced.
		var err error
		tmpDir, err = os.MkdirTemp("", "")
		if err != nil {
			return err
		}
	}
	defer func() {
		if err := sendBepDataToBeaverHubIfNeeded(filepath.Join(tmpDir, bepFileBasename)); err != nil {
			// Retry.
			if err := sendBepDataToBeaverHubIfNeeded(filepath.Join(tmpDir, bepFileBasename)); err != nil && d.debug {
				log.Printf("Internal Error: Sending BEP file to beaver hub failed - %v", err)
			}
		}
		if !buildutil.CrdbTestBuild {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	targets, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	cross := mustGetFlagString(cmd, crossFlag)
	lint := mustGetFlagBool(cmd, lintFlag)
	dockerArgs := mustGetFlagStringArray(cmd, dockerArgsFlag)

	args, buildTargets, err := d.getBasicBuildArgs(ctx, targets)
	if err != nil {
		return err
	}
	if lint {
		args = append(args, nogoEnableFlag)
	}
	args = append(args, additionalBazelArgs...)
	configArgs := getConfigArgs(args)

	if err := d.assertNoLinkedNpmDeps(buildTargets); err != nil {
		return err
	}

	if cross == "" {
		// Do not log --build_event_binary_file=... because it is not relevant to the actual call
		// from the user perspective.
		logCommand("bazel", args...)
		if buildutil.CrdbTestBuild {
			args = append(args, "--build_event_binary_file=/tmp/path")
		} else {
			args = append(args, fmt.Sprintf("--build_event_binary_file=%s", filepath.Join(tmpDir, bepFileBasename)))
		}
		if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
			return err
		}
		return d.stageArtifacts(ctx, buildTargets, configArgs)
	}
	volume := mustGetFlagString(cmd, volumeFlag)
	cross = "cross" + cross
	return d.crossBuild(ctx, args, buildTargets, cross, volume, dockerArgs)
}

func (d *dev) crossBuild(
	ctx context.Context,
	bazelArgs []string,
	targets []buildTarget,
	crossConfig string,
	volume string,
	dockerArgs []string,
) error {
	bazelArgs = append(bazelArgs, fmt.Sprintf("--config=%s", crossConfig), "--config=nolintonbuild", "-c", "opt")
	configArgs := getConfigArgs(bazelArgs)
	dockerArgs, err := d.getDockerRunArgs(ctx, volume, false, dockerArgs)
	if err != nil {
		return err
	}
	// Construct a script that builds the binaries and copies them
	// to the appropriate location in /artifacts.
	var script strings.Builder
	script.WriteString("set -euxo pipefail\n")
	script.WriteString(fmt.Sprintf("bazel %s\n", shellescape.QuoteCommand(bazelArgs)))
	var bazelBinSet bool
	script.WriteString("set +x\n")
	for _, target := range targets {
		if target.kind == "geos" {
			// Cross-build will never force-build geos.
			script.WriteString(fmt.Sprintf("EXECROOT=$(bazel info execution_root %s)\n", shellescape.QuoteCommand(configArgs)))
			libDir := "lib"
			if strings.Contains(crossConfig, "windows") {
				libDir = "bin"
			}
			script.WriteString(fmt.Sprintf("for LIB in `ls $EXECROOT/external/archived_cdep_libgeos_%s/%s`; do\n", strings.TrimPrefix(crossConfig, "cross"), libDir))
			script.WriteString(fmt.Sprintf("cp $EXECROOT/external/archived_cdep_libgeos_%s/%s/$LIB /tmp\n", strings.TrimPrefix(crossConfig, "cross"), libDir))
			script.WriteString("chmod a+w /tmp/$LIB\n")
			script.WriteString("mv /tmp/$LIB /artifacts\n")
			script.WriteString(fmt.Sprintf("echo \"Successfully built target %s at artifacts/$LIB\"\n", target.fullName))
			script.WriteString("done")
			continue
		}
		if target.kind == "go_binary" || target.kind == "go_test" {
			if !bazelBinSet {
				script.WriteString(fmt.Sprintf("BAZELBIN=$(bazel info bazel-bin %s)\n", shellescape.QuoteCommand(configArgs)))
				bazelBinSet = true
			}
			output := bazelutil.OutputOfBinaryRule(target.fullName, strings.Contains(crossConfig, "windows"))
			baseOutput := filepath.Base(output)
			script.WriteString(fmt.Sprintf("cp -R $BAZELBIN/%s /tmp\n", output))
			script.WriteString(fmt.Sprintf("chmod a+w /tmp/%s\n", baseOutput))
			script.WriteString(fmt.Sprintf("mv /tmp/%s /artifacts\n\n", baseOutput))
			script.WriteString(fmt.Sprintf("echo \"Successfully built target %s at artifacts/%s\"\n", target.fullName, baseOutput))
			continue
		}
		// Catch-all case: run the target being built under `realpath`
		// to figure out where to copy the binary from.
		// NB: For test targets, the `stdout` output from `bazel run` is
		// going to have some extra garbage. We grep ^/ to select out
		// only the filename we're looking for.
		script.WriteString(fmt.Sprintf("BIN=$(bazel run %s %s --run_under=realpath | grep ^/ | tail -n1)\n", target.fullName, shellescape.QuoteCommand(configArgs)))
		script.WriteString("cp $BIN /tmp\n")
		script.WriteString("chmod a+w /tmp/$(basename $BIN)\n")
		script.WriteString("mv /tmp/$(basename $BIN) /artifacts\n")
		script.WriteString(fmt.Sprintf("echo \"Successfully built binary for target %s at artifacts/$(basename $BIN)\"\n", target.fullName))
	}
	_, err = d.exec.CommandContextWithInput(ctx, script.String(), "docker", dockerArgs...)
	return err
}

func (d *dev) stageArtifacts(
	ctx context.Context, targets []buildTarget, configArgs []string,
) error {
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	// Create the bin directory.
	if err = d.os.MkdirAll(path.Join(workspace, "bin")); err != nil {
		return err
	}
	bazelBin, err := d.getBazelBin(ctx, configArgs)
	if err != nil {
		return err
	}

	for _, target := range targets {
		if target.kind != "go_binary" && target.kind != "geos" {
			// Skip staging for these.
			continue
		}
		if target.kind == "geos" {
			if err := d.os.MkdirAll(path.Join(workspace, "lib")); err != nil {
				return err
			}
			// Libraries are unusual in that they end up in lib/ rather than bin/.
			archived, err := d.getArchivedCdepString(bazelBin)
			if err != nil {
				return err
			}
			var geosDir string
			if archived != "" {
				execRoot, err := d.getExecutionRoot(ctx, configArgs)
				if err != nil {
					return err
				}
				geosDir = filepath.Join(execRoot, "external", "archived_cdep_libgeos_"+archived)
			} else {
				geosDir = filepath.Join(bazelBin, "c-deps", "libgeos_foreign")
			}
			libDir := "lib"
			var ext string
			if runtime.GOOS == "windows" {
				ext = "dll"
				// Shared libraries end up in the "bin" dir on Windows.
				libDir = "bin"
			} else if runtime.GOOS == "darwin" {
				ext = "dylib"
			} else {
				ext = "so"
			}
			for _, whichLib := range []string{"libgeos.", "libgeos_c."} {
				baseName := whichLib + ext
				dst := filepath.Join(workspace, "lib", baseName)
				if err := d.os.CopyFile(filepath.Join(geosDir, libDir, baseName), dst); err != nil {
					return err
				}
				successfullyBuilt(workspace, "library", target.fullName, dst)
			}
			continue
		}
		binaryPath := filepath.Join(bazelBin, bazelutil.OutputOfBinaryRule(target.fullName, runtime.GOOS == "windows"))
		base := targetToBinBasename(target.fullName)
		var copyPaths []string
		// Binaries beginning with the string "cockroach" go right at
		// the top of the workspace; others go in the `bin` directory.
		if strings.HasPrefix(base, "cockroach") {
			copyPaths = append(copyPaths, filepath.Join(workspace, base))
			if strings.HasPrefix(base, "cockroach-short") {
				copyPaths = append(copyPaths, filepath.Join(workspace, "cockroach"))
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

			copyPaths = append(copyPaths,
				filepath.Join(workspace, "bin", "dev-versions", fmt.Sprintf("dev.%s", devVersion)))
		} else {
			copyPaths = append(copyPaths, filepath.Join(workspace, "bin", base))
		}

		// Copy from binaryPath -> copyPath, clear out detritus, if any.
		for _, copyPath := range copyPaths {
			if err := d.os.Remove(copyPath); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err := d.os.CopyFile(binaryPath, copyPath); err != nil {
				return err
			}
			successfullyBuilt(workspace, "binary", target.fullName, copyPath)
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
	addCommonBazelArguments(&args)

	canDisableNogo := true
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
				if fullTargetName == geosTarget {
					// We need to build test_force_build_cdeps.txt so we can
					// check where the geos libraries will end up when built.
					args = append(args, "//build/bazelutil:test_force_build_cdeps")
					// Note the "kind" is explicitly set to "geos" in this case.
					buildTargets = append(buildTargets, buildTarget{fullName: geosTarget, kind: "geos"})
					continue
				}
				buildTargets = append(buildTargets, buildTarget{fullName: fullTargetName, kind: typ})
				if typ == "go_test" || typ == "go_transition_test" || typ == "test_suite" {
					shouldBuildWithTestConfig = true
				}
				if strings.HasPrefix(fullTargetName, "//") {
					canDisableNogo = false
				}
			}
			continue
		}

		aliased, ok := buildTargetMapping[target]
		if !ok {
			return nil, nil, fmt.Errorf("unrecognized target: %s", target)
		}

		args = append(args, aliased)
		if aliased == "//pkg:all_tests" {
			buildTargets = append(buildTargets, buildTarget{fullName: aliased, kind: "test_suite"})
			shouldBuildWithTestConfig = true
		} else if aliased == "//c-deps:libgeos" {
			args = append(args, "//build/bazelutil:test_force_build_cdeps")
			buildTargets = append(buildTargets, buildTarget{fullName: aliased, kind: "geos"})
		} else {
			buildTargets = append(buildTargets, buildTarget{fullName: aliased, kind: "go_binary"})
		}
		if strings.HasPrefix(aliased, "//") && aliased != devTarget {
			canDisableNogo = false
		}
	}

	if shouldBuildWithTestConfig {
		args = append(args, "--config=test")
	}
	if canDisableNogo {
		args = append(args, nogoDisableFlag)
	}
	return args, buildTargets, nil
}

// Given a list of Bazel arguments, find the ones that represent a "config"
// (either --config or -c) and return all of these. This is used to find
// the appropriate bazel-bin for any invocation.
func getConfigArgs(args []string) (ret []string) {
	var addNext bool
	for _, arg := range args {
		if addNext {
			ret = append(ret, arg)
			addNext = false
		} else if arg == "--config" || arg == "--compilation_mode" || arg == "-c" {
			ret = append(ret, arg)
			addNext = true
		} else if strings.HasPrefix(arg, "--config=") || strings.HasPrefix(arg, "--compilation_mode=") {
			ret = append(ret, arg)
		}
	}
	return
}

func successfullyBuilt(workspace, what, target, path string) {
	rel, err := filepath.Rel(workspace, path)
	if err != nil {
		rel = path
	}
	log.Printf("Successfully built %s for target %s at %s", what, target, rel)
}
