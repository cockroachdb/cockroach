// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/alessio/shellescape"
	"github.com/spf13/cobra"
)

const (
	clusterFlag    = "cluster"
	stressArgsFlag = "stress-args"
	stressTarget   = "@com_github_cockroachdb_stress//:stress"
)

func makeRoachprodStressCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	roachprodStressCmd := &cobra.Command{
		Use:     "roachprod-stress <pkg>",
		Short:   "stress the given tests on the given roachprod cluster",
		Long:    "stress the given tests on the given roachprod cluster.",
		Example: `dev roachprod-stress ./pkg/sql/importer --cluster my_cluster --stress-args '-arg1 -arg2' -- -test.run="TestMultiNodeExportStmt"`,
		Args:    cobra.MinimumNArgs(1),
		RunE:    runE,
	}
	roachprodStressCmd.Flags().String(stressArgsFlag, "", "additional arguments to pass to stress")
	roachprodStressCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	roachprodStressCmd.Flags().String(clusterFlag, "", "the name of the cluster (must be set)")
	roachprodStressCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	roachprodStressCmd.Flags().Bool(deadlockFlag, false, "run tests using the deadlock detector")
	return roachprodStressCmd
}

func (d *dev) roachprodStress(cmd *cobra.Command, commandLine []string) error {
	ctx := cmd.Context()
	var (
		cluster       = mustGetFlagString(cmd, clusterFlag)
		volume        = mustGetFlagString(cmd, volumeFlag)
		race          = mustGetFlagBool(cmd, raceFlag)
		deadlock      = mustGetFlagBool(cmd, deadlockFlag)
		stressCmdArgs = mustGetFlagString(cmd, stressArgsFlag)
	)
	if cluster == "" {
		return fmt.Errorf("must provide --cluster (you can create one via: `roachprod create $USER-stress -n 20 --gce-machine-type=n1-standard-8 --local-ssd=false`)")
	}
	pkgs, testArgs := splitArgsAtDash(cmd, commandLine)
	if len(pkgs) != 1 {
		return fmt.Errorf("must provide exactly one test target like ./pkg/cmd/dev")
	}
	// Find the target we need to build.
	pkg := pkgs[0]
	pkg = strings.TrimPrefix(pkg, "//")
	pkg = strings.TrimPrefix(pkg, "./")
	pkg = strings.TrimRight(pkg, "/")
	if !strings.HasPrefix(pkg, "pkg/") {
		return fmt.Errorf("malformed package %q, expecting pkg/{...}", pkg)
	}

	var testTarget string
	if strings.Contains(pkg, ":") {
		testTarget = pkg
	} else {
		queryArgs := []string{"query", fmt.Sprintf("kind(go_test, //%s:all)", pkg), "--output=label_kind"}
		labelKind, queryErr := d.exec.CommandContextSilent(ctx, "bazel", queryArgs...)
		if queryErr != nil {
			return fmt.Errorf("could not run `bazel %s` (%w)", shellescape.QuoteCommand(queryArgs), queryErr)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(labelKind)), "\n") {
			fields := strings.Fields(line)
			if testTarget != "" {
				return fmt.Errorf("expected a single test target; got both %s and %s. Please specify in your command, like `dev roachprod-stress %s", testTarget, fields[len(fields)-1], testTarget)
			}
			testTarget = fields[len(fields)-1]
			if fields[0] != "go_test" {
				return fmt.Errorf("target %s is of target type %s; expected go_test", testTarget, fields[0])
			}
		}
	}

	// List of targets we need to cross-build.
	crossTargets := []string{testTarget, stressTarget}
	// Check whether this target depends on libgeos.
	dependsOnGeos := false
	queryArgs := []string{"query", fmt.Sprintf("somepath(%s, //c-deps:libgeos)", testTarget)}
	queryOutput, err := d.exec.CommandContextSilent(ctx, "bazel", queryArgs...)
	if err != nil {
		return fmt.Errorf("could not run `bazel %s` (%w)", shellescape.QuoteCommand(queryArgs), err)
	}
	if strings.TrimSpace(string(queryOutput)) != "" {
		// If the test depends on geos we additionally want to cross-build it.
		dependsOnGeos = true
		crossTargets = append(crossTargets, "//c-deps:libgeos")
	}

	crossArgs, targets, err := d.getBasicBuildArgs(ctx, crossTargets)
	if err != nil {
		return err
	}
	if race {
		crossArgs = append(crossArgs, "--config=race")
	}
	if deadlock {
		crossArgs = append(crossArgs, "--define", "gotags=bazel,gss,deadlock")
	}
	err = d.crossBuild(ctx, crossArgs, targets, "crosslinux", volume, nil)
	if err != nil {
		return err
	}

	// Build roachprod-stress.
	args, roachprodStressTarget, err := d.getBasicBuildArgs(ctx, []string{"//pkg/cmd/roachprod-stress"})
	if err != nil {
		return err
	}
	if _, err := d.exec.CommandContextSilent(ctx, "bazel", args...); err != nil {
		return err
	}
	if err := d.stageArtifacts(ctx, roachprodStressTarget, []string{}); err != nil {
		return err
	}

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}

	libdir := path.Join(workspace, "artifacts", "libgeos", "lib")
	if dependsOnGeos {
		if err = d.os.MkdirAll(libdir); err != nil {
			return err
		}
		for _, libWithExt := range []string{"libgeos.so", "libgeos_c.so"} {
			src := filepath.Join(workspace, "artifacts", libWithExt)
			dst := filepath.Join(libdir, libWithExt)
			if err := d.os.CopyFile(src, dst); err != nil {
				return err
			}
			if err := d.os.Remove(src); err != nil {
				return err
			}
		}
	}

	testTargetBasename := strings.Split(targets[0].fullName, ":")[1]
	// Run roachprod-stress.
	roachprodStressArgs := []string{cluster, fmt.Sprintf("./%s", pkg), "-testbin", filepath.Join(workspace, "artifacts", testTargetBasename), "-stressbin", filepath.Join(workspace, "artifacts", "stress"), "-libdir", libdir}
	roachprodStressArgs = append(roachprodStressArgs, strings.Fields(stressCmdArgs)...)
	roachprodStressArgs = append(roachprodStressArgs, "--")
	roachprodStressArgs = append(roachprodStressArgs, testArgs...)
	return d.exec.CommandContextInheritingStdStreams(ctx, filepath.Join(workspace, "bin", "roachprod-stress"), roachprodStressArgs...)
}
