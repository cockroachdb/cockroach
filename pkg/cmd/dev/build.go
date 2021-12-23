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

	"github.com/alessio/shellescape"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/spf13/cobra"
)

const (
	crossFlag              = "cross"
	hoistGeneratedCodeFlag = "hoist-generated-code"
)

type buildTarget struct {
	fullName   string
	isGoBinary bool
}

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
	buildCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	buildCmd.Flags().String(crossFlag, "", `
        Turns on cross-compilation. Builds the binary using the builder image w/ Docker.
        You can optionally set a config, as in --cross=windows.
        Defaults to linux if not specified. The config should be the name of a
        build configuration specified in .bazelrc, minus the "cross" prefix.`)
	buildCmd.Flags().Bool(hoistGeneratedCodeFlag, false, "hoist generated code out of the Bazel sandbox into the workspace")
	buildCmd.Flags().Lookup(crossFlag).NoOptDefVal = "linux"
	addCommonBuildFlags(buildCmd)
	return buildCmd
}

// TODO(irfansharif): Add grouping shorthands like "all" or "bins", etc.
// TODO(irfansharif): Make sure all the relevant binary targets are defined
// above, and in usage docs.

var buildTargetMapping = map[string]string{
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

// Do not hoist any files from the sandbox into the workspace that have any of
// the following basenames.
var doNotHoistFiles = map[string]bool{
	"gen-reports-TxnStateTransitions_stateNoTxn_write_reports.go": true,
}

func (d *dev) build(cmd *cobra.Command, commandLine []string) error {
	targets, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	cross := mustGetFlagString(cmd, crossFlag)
	hoistGeneratedCode := mustGetFlagBool(cmd, hoistGeneratedCodeFlag)

	args, buildTargets, err := d.getBasicBuildArgs(ctx, targets)
	if err != nil {
		return err
	}
	args = append(args, additionalBazelArgs...)

	if cross == "" {
		logCommand("bazel", args...)
		if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
			return err
		}
		return d.stageArtifacts(ctx, buildTargets, hoistGeneratedCode)
	}
	// Cross-compilation case.
	for _, target := range buildTargets {
		if !target.isGoBinary {
			// We can't cross-compile these targets because we can't be sure where
			// Bazel is going to stage their output files.
			return fmt.Errorf("cannot cross-compile target %s because it is not a go binary", target.fullName)
		}
	}
	cross = "cross" + cross
	volume := mustGetFlagString(cmd, volumeFlag)
	args = append(args, fmt.Sprintf("--config=%s", cross), "--config=ci")
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
	script.WriteString(fmt.Sprintf("BAZELBIN=`bazel info bazel-bin --color=no --config=%s --config=ci`\n", cross))
	for _, target := range buildTargets {
		script.WriteString(fmt.Sprintf("cp $BAZELBIN/%s /artifacts\n", bazelutil.OutputOfBinaryRule(target.fullName)))
	}
	_, err = d.exec.CommandContextWithInput(ctx, script.String(), "docker", dockerArgs...)
	if err != nil {
		return err
	}
	for _, target := range buildTargets {
		logSuccessfulBuild(target.fullName, filepath.Join("artifacts", targetToBinBasename(target.fullName)))
	}
	return nil
}

func (d *dev) stageArtifacts(
	ctx context.Context, targets []buildTarget, hoistGeneratedCode bool,
) error {
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
		if !target.isGoBinary {
			// Skip staging for these.
			continue
		}
		binaryPath := filepath.Join(bazelBin, bazelutil.OutputOfBinaryRule(target.fullName))
		base := targetToBinBasename(target.fullName)
		var symlinkPath string
		// Binaries beginning with the string "cockroach" go right at
		// the top of the workspace; others go in the `bin` directory.
		if strings.HasPrefix(base, "cockroach") {
			symlinkPath = filepath.Join(workspace, base)
		} else {
			symlinkPath = filepath.Join(workspace, "bin", base)
		}

		// Symlink from binaryPath -> symlinkPath
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
		logSuccessfulBuild(target.fullName, rel)
	}

	if hoistGeneratedCode {
		// Clean up ignored .go files. Do this by listing all the
		// ignored files and filtering out irrelevant ones.
		// We do this to get rid of stale generated files that might
		// confuse IDE's, especially if you switch between branches that
		// have different generated code.
		lines, err := d.exec.CommandContextSilent(ctx, "git", "status", "--ignored", "--short", filepath.Join(workspace, "pkg"))
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(lines), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || !strings.HasPrefix(line, "!! ") {
				continue
			}
			filename := strings.TrimPrefix(line, "!! ")
			if !strings.HasSuffix(filename, ".go") || strings.Contains(filename, "zcgo_flags") {
				continue
			}
			if err := d.os.Remove(filename); err != nil {
				return err
			}
		}
		// Enumerate generated .go files in the sandbox so we can hoist
		// them out.
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
			if strings.Contains(file, "_gomock_gopath") || strings.HasSuffix(file, "_gomock_prog.go") {
				continue
			}
			// Any files in doNotHoistFiles shouldn't be hoisted.
			if _, doNotHoist := doNotHoistFiles[filepath.Base(file)]; doNotHoist {
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
func (d *dev) getBasicBuildArgs(
	ctx context.Context, targets []string,
) (args []string, buildTargets []buildTarget, err error) {
	if len(targets) == 0 {
		// Default to building the cockroach binary.
		targets = append(targets, "cockroach")
	}

	args = append(args, "build")
	args = append(args, mustGetRemoteCacheArgs(remoteCacheAddr)...)
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
				err = fmt.Errorf("could not run `bazel %s` (%w)", shellescape.QuoteCommand(queryArgs), queryErr)
				return
			}
			for _, line := range strings.Split(strings.TrimSpace(string(labelKind)), "\n") {
				fields := strings.Fields(line)
				fullTargetName := fields[len(fields)-1]
				typ := fields[0]
				if typ != "go_binary" && typ != "go_library" && typ != "go_test" {
					// Skip all targets besides go_binary targets, go_library
					// targets, and go_test targets. Notably this does not
					// include go_proto_library targets which at this point
					// cannot be built standalone.
					continue
				}
				args = append(args, fullTargetName)
				buildTargets = append(buildTargets, buildTarget{fullName: fullTargetName, isGoBinary: typ == "go_binary"})
				if typ == "go_test" {
					shouldBuildWithTestConfig = true
				}
			}
			continue
		}
		aliased, ok := buildTargetMapping[target]
		if !ok {
			err = fmt.Errorf("unrecognized target: %s", target)
			return
		}

		args = append(args, aliased)
		buildTargets = append(buildTargets, buildTarget{fullName: aliased, isGoBinary: true})
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

	return
}

func logSuccessfulBuild(target, rel string) {
	log.Printf("Successfully built binary for target %s at %s", target, rel)
}
