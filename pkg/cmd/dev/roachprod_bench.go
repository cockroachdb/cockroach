// Copyright 2022 The Cockroach Authors.
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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
)

const (
	benchArgsFlag = "bench-args"
	compressFlag  = "compress"
	buildHashFlag = "build-hash"
	batchSizeFlag = "batch-size"
)

type packageTemplateData struct {
	RunfilesDirName string
	TargetBinName   string
}

type stageOutputTemplateData struct {
	packageTemplateData
	BazelOutputDir string
	StageDir       string
	TargetName     string
	RunScript      string
}

type runTemplateData struct {
	packageTemplateData
	PackageDir string
}

const stageOutputTemplateScript = `
mkdir -p #{.StageDir#}
cp -Lr #{.BazelOutputDir#}/#{.RunfilesDirName#} #{.StageDir#}/#{.RunfilesDirName#}
cp -Lr #{.BazelOutputDir#}/#{.TargetBinName#} #{.StageDir#}/#{.TargetBinName#}

echo #{shesc .RunScript#} > #{.StageDir#}/run.sh
chmod +x #{.StageDir#}/run.sh

echo "Successfully staged target #{.TargetName#}"
`

const packageTemplateScript = `
echo Packaging staged build output...
cd #{.ArtifactsDir#}
tar -chf bin.tar pkg
rm -rf pkg
`

const runTemplateScript = `
export RUNFILES_DIR=$(pwd)/#{.RunfilesDirName#}
export TEST_WORKSPACE=com_github_cockroachdb_cockroach/#{.PackageDir#}

find . -maxdepth 1 -type l -delete
ln -f -s ./#{.RunfilesDirName#}/com_github_cockroachdb_cockroach/#{.PackageDir#}/* .
./#{.TargetBinName#} "$@"
`

func makeRoachprodBenchCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	roachprodBenchCmd := &cobra.Command{
		Use:     "roachprod-bench-wrapper <pkg>",
		Short:   "invokes pkg/cmd/roachprod-bench to parallelize execution of specified microbenchmarks",
		Long:    "invokes pkg/cmd/roachprod-bench to parallelize execution of specified microbenchmarks",
		Example: `dev roachprod-bench-wrapper ./pkg/sql/importer --cluster my_cluster`,
		Args:    cobra.MinimumNArgs(1),
		RunE:    runE,
	}
	roachprodBenchCmd.Flags().String(benchArgsFlag, "", "additional arguments to pass to roachbench")
	roachprodBenchCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	roachprodBenchCmd.Flags().String(clusterFlag, "", "the name of the cluster (must be set)")
	roachprodBenchCmd.Flags().String(buildHashFlag, "", "override the build hash for the build output")
	roachprodBenchCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	roachprodBenchCmd.Flags().Bool(compressFlag, true, "compress the output of the benchmarks binaries")
	roachprodBenchCmd.Flags().Int(batchSizeFlag, 128, "the number of packages to build per batch")
	roachprodBenchCmd.Flags().String(crossFlag, "crosslinux", "the cross build target to use")
	return roachprodBenchCmd
}

func (d *dev) roachprodBench(cmd *cobra.Command, commandLine []string) error {
	ctx := cmd.Context()
	var (
		cluster   = mustGetFlagString(cmd, clusterFlag)
		volume    = mustGetFlagString(cmd, volumeFlag)
		benchArgs = mustGetFlagString(cmd, benchArgsFlag)
		race      = mustGetFlagBool(cmd, raceFlag)
		compress  = mustGetFlagBool(cmd, compressFlag)
		batchSize = mustGetConstrainedFlagInt(cmd, batchSizeFlag, func(v int) error {
			if v <= 0 {
				return fmt.Errorf("%s must be greater than zero", batchSizeFlag)
			}
			return nil
		})
		crossConfig = mustGetFlagString(cmd, crossFlag)
		buildHash   = mustGetFlagString(cmd, buildHashFlag)
	)

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}

	if cluster == "" {
		return fmt.Errorf("must provide --cluster (e.g., `roachprod create $USER-bench -n 8 --gce-machine-type=n2d-standard-8 --local-ssd=true`)")
	}
	packages, testArgs := splitArgsAtDash(cmd, commandLine)
	if len(packages) != 1 {
		return fmt.Errorf("expected a single benchmark target specification; e.g., ./pkg/util/... or ./pkg/cmd/dev")
	}

	// Find the target we need to build.
	pkg := packages[0]
	pkg = strings.TrimPrefix(pkg, "//")
	pkg = strings.TrimPrefix(pkg, "./")
	pkg = strings.TrimRight(pkg, "/")
	if !strings.HasPrefix(pkg, "pkg/") {
		return fmt.Errorf("malformed package %q, expecting pkg/{...}", pkg)
	}

	// Find all the test targets with the given package argument.
	queryArgs := []string{"query", fmt.Sprintf("kind(go_test, //%s:all)", pkg), "--output=label_kind"}
	labelKind, queryErr := d.exec.CommandContextSilent(ctx, "bazel", queryArgs...)
	if queryErr != nil {
		return fmt.Errorf("could not run `bazel %s` (%w)", shellescape.QuoteCommand(queryArgs), queryErr)
	}
	testTargets := make([]string, 0)
	for _, line := range strings.Split(strings.TrimSpace(string(labelKind)), "\n") {
		fields := strings.Fields(line)
		testTarget := fields[len(fields)-1]
		if fields[0] != "go_test" {
			return fmt.Errorf("target %s is of target type %s; expected go_test", testTarget, fields[0])
		}
		testTargets = append(testTargets, testTarget)
	}

	if len(testTargets) == 0 {
		return fmt.Errorf("no test targets found for package %s", pkg)
	}

	// Generate a unique build hash for the given targets and git revision.
	if buildHash == "" {
		stdout, cmdErr := d.exec.CommandContextSilent(ctx, "git", "rev-parse", "HEAD")
		if cmdErr != nil {
			return cmdErr
		}
		gitRevision := strings.TrimSpace(string(stdout))
		buildHashSHA := sha256.New()
		buildHashSHA.Write([]byte(gitRevision))
		for _, target := range testTargets {
			buildHashSHA.Write([]byte(target))
		}
		buildHash = hex.EncodeToString(buildHashSHA.Sum(nil)[:])[:8]
	}

	// Assume that we need LibGEOS.
	if err = d.buildGeos(ctx, volume); err != nil {
		return err
	}

	// Clear any old artifacts.
	outputPrefix := fmt.Sprintf("roachbench/%s", buildHash)
	binTar := fmt.Sprintf("artifacts/%s/bin.tar", outputPrefix)
	_, _ = d.os.Remove(binTar), d.os.Remove(binTar+".gz")

	// Build the test targets.
	var manifest strings.Builder
	artifactsDir := fmt.Sprintf("/artifacts/%s", outputPrefix)
	dockerArgs, err := d.getDockerRunArgs(ctx, volume, false)
	if err != nil {
		return err
	}
	crossTargets := testTargets
	for i := 0; i < len(crossTargets); i += batchSize {
		targetBatch := crossTargets[i:min(i+batchSize, len(crossTargets))]
		log.Printf("Building batch %v\n", targetBatch)

		// Build packages.
		crossArgs, targets, argErr := d.getBasicBuildArgs(ctx, targetBatch)
		if argErr != nil {
			return argErr
		}
		if race {
			crossArgs = append(crossArgs, "--config=race")
		}
		err = d.crossBuildTests(ctx, dockerArgs, crossArgs, targets, crossConfig, artifactsDir)
		if err != nil {
			return err
		}

		// Add package name to the manifest file.
		for _, target := range targetBatch {
			targetDir := targetToDir(target)
			manifest.WriteString(fmt.Sprintf("%s\n", targetDir))
		}
	}

	err = d.packageTestArtifacts(ctx, dockerArgs, artifactsDir)
	if err != nil {
		return err
	}

	if compress {
		if compErr := compressFile(binTar); compErr != nil {
			return compErr
		}
		if remErr := os.Remove(binTar); remErr != nil {
			return remErr
		}
	}

	err = os.WriteFile(fmt.Sprintf("artifacts/%s/roachbench.manifest", outputPrefix), []byte(manifest.String()), 0644)
	if err != nil {
		return err
	}

	// Build roachprod-bench.
	args, roachprodBenchTarget, err := d.getBasicBuildArgs(ctx, []string{"//pkg/cmd/roachprod-bench"})
	if err != nil {
		return err
	}
	if _, err = d.exec.CommandContextSilent(ctx, "bazel", args...); err != nil {
		return err
	}
	if err = d.stageArtifacts(ctx, roachprodBenchTarget); err != nil {
		return err
	}

	libdir := path.Join(workspace, "artifacts", "libgeos", "lib")
	if err = d.os.MkdirAll(libdir); err != nil {
		return err
	}
	for _, libWithExt := range []string{"libgeos.so", "libgeos_c.so"} {
		src := filepath.Join(workspace, "artifacts", libWithExt)
		dst := filepath.Join(libdir, libWithExt)
		if err = d.os.CopyFile(src, dst); err != nil {
			return err
		}
		if err = d.os.Remove(src); err != nil {
			return err
		}
	}

	roachprodBenchArgs := []string{fmt.Sprintf("./artifacts/%s", outputPrefix), "-cluster", cluster, "-libdir", libdir}
	roachprodBenchArgs = append(roachprodBenchArgs, strings.Fields(benchArgs)...)
	roachprodBenchArgs = append(roachprodBenchArgs, "--")
	roachprodBenchArgs = append(roachprodBenchArgs, testArgs...)
	log.Printf("Running roachprod-bench with args: %s", strings.Join(roachprodBenchArgs, " "))
	return d.exec.CommandContextInheritingStdStreams(ctx, filepath.Join(workspace, "bin", "roachprod-bench"), roachprodBenchArgs...)
}

func (d *dev) crossBuildTests(
	ctx context.Context,
	dockerArgs []string,
	bazelArgs []string,
	targets []buildTarget,
	crossConfig string,
	artifactsDir string,
) error {
	bazelArgs = append(bazelArgs, fmt.Sprintf("--config=%s", crossConfig), "--config=ci")
	configArgs := getConfigArgs(bazelArgs)
	stageOutputTemplate, err := createTemplate("stage", stageOutputTemplateScript)
	if err != nil {
		return err
	}
	runTemplate, err := createTemplate("run", runTemplateScript)
	if err != nil {
		return err
	}

	// Construct a script that builds the binaries and copies them
	// to the appropriate location in /artifacts.
	var script strings.Builder
	var bazelBinSet bool
	script.WriteString("set -euxo pipefail\n")
	script.WriteString(fmt.Sprintf("bazel %s\n", shellescape.QuoteCommand(bazelArgs)))
	script.WriteString("set +x\n")
	for _, target := range targets {
		if target.kind == "go_binary" || target.kind == "go_test" {
			if !bazelBinSet {
				script.WriteString(fmt.Sprintf("BAZELBIN=$(bazel info bazel-bin %s)\n", shellescape.QuoteCommand(configArgs)))
				bazelBinSet = true
			}
			bazelBinOutput := bazelutil.OutputOfBinaryRule(target.fullName, strings.Contains(crossConfig, "windows"))
			templateData := packageTemplateData{
				RunfilesDirName: fmt.Sprintf("%s.runfiles", targetToBinBasename(target.fullName)),
				TargetBinName:   targetToBinBasename(target.fullName),
			}

			var runBuf strings.Builder
			if tErr := runTemplate.Execute(&runBuf, runTemplateData{
				packageTemplateData: templateData,
				PackageDir:          targetToDir(target.fullName),
			}); tErr != nil {
				return tErr
			}

			var buildBuf strings.Builder
			if tErr := stageOutputTemplate.Execute(&buildBuf, stageOutputTemplateData{
				packageTemplateData: templateData,
				BazelOutputDir:      fmt.Sprintf("$BAZELBIN/%s", bazelBinOutput[:strings.LastIndex(bazelBinOutput, "/")]),
				RunScript:           runBuf.String(),
				StageDir:            fmt.Sprintf("%s/%s/bin/", artifactsDir, targetToDir(target.fullName)),
				TargetName:          target.fullName,
			}); tErr != nil {
				return tErr
			}

			script.WriteString(buildBuf.String())
		}
	}
	_, err = d.exec.CommandContextWithInput(ctx, script.String(), "docker", dockerArgs...)
	return err
}

func (d *dev) packageTestArtifacts(
	ctx context.Context, dockerArgs []string, artifactsDir string,
) error {
	var packageBuf strings.Builder
	packageTemplate, err := createTemplate("package", packageTemplateScript)
	if err != nil {
		return err
	}
	if tErr := packageTemplate.Execute(&packageBuf, struct{ ArtifactsDir string }{
		ArtifactsDir: artifactsDir,
	}); tErr != nil {
		return tErr
	}
	_, err = d.exec.CommandContextWithInput(ctx, packageBuf.String(), "docker", dockerArgs...)
	return err
}

func (d *dev) buildGeos(ctx context.Context, volume string) error {
	crossArgs, targets, err := d.getBasicBuildArgs(ctx, []string{"//c-deps:libgeos"})
	if err != nil {
		return err
	}
	return d.crossBuild(ctx, crossArgs, targets, "crosslinux", volume)
}

func createTemplate(name string, script string) (*template.Template, error) {
	return template.New(name).
		Funcs(template.FuncMap{"shesc": func(i interface{}) string {
			return shellescape.Quote(fmt.Sprint(i))
		}}).
		Delims("#{", "#}").
		Parse(script)
}

func targetToDir(target string) string {
	return strings.TrimPrefix(target[:strings.LastIndex(target, ":")], "//")
}

func compressFile(filePath string) (err error) {
	inputFile, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer func() {
		err = combineErrors(inputFile.Close(), err)
	}()
	compressedFile, err := os.Create(filePath + ".gz")
	if err != nil {
		return
	}
	defer func() {
		err = combineErrors(compressedFile.Close(), err)
	}()
	writer := pgzip.NewWriter(compressedFile)
	_, err = io.Copy(writer, inputFile)
	if err != nil {
		return
	}
	err = writer.Close()
	return
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func combineErrors(err error, otherErr error) error {
	if err == nil {
		return otherErr
	}
	if otherErr != nil {
		// nolint:errwrap dev package prohibits import of cockroach errors due to its size.
		return fmt.Errorf("multiple errors occurred: %s; %s", err.Error(), otherErr.Error())
	}
	return err
}
