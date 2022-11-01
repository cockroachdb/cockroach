package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
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
	benchArgsFlag = "bench-args"
	compressFlag  = "compress"
	batchSize     = 128
)

func makeRoachprodBenchCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	roachprodBenchCmd := &cobra.Command{
		Use:     "roachprod-bench <pkg>",
		Short:   "run the given benchmarks on the given roachprod cluster",
		Long:    "run the given benchmarks on the given roachprod cluster.",
		Example: `dev roachprod-bench ./pkg/sql/importer --cluster my_cluster`,
		Args:    cobra.MinimumNArgs(1),
		RunE:    runE,
	}
	roachprodBenchCmd.Flags().String(benchArgsFlag, "", "additional arguments to pass to roachbench")
	roachprodBenchCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	roachprodBenchCmd.Flags().String(clusterFlag, "", "the name of the cluster (must be set)")
	roachprodBenchCmd.Flags().Bool(raceFlag, false, "run tests using race builds")
	roachprodBenchCmd.Flags().Bool(compressFlag, false, "compress the output of the benchmarks binaries")

	return roachprodBenchCmd
}

func (d *dev) roachprodBench(cmd *cobra.Command, commandLine []string) error {
	ctx := cmd.Context()
	var (
		cluster   = mustGetFlagString(cmd, clusterFlag)
		volume    = mustGetFlagString(cmd, volumeFlag)
		benchArgs = mustGetFlagString(cmd, benchArgsFlag)
		race      = mustGetFlagBool(cmd, raceFlag)
		comress   = mustGetFlagBool(cmd, compressFlag)
	)

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}

	if cluster == "" {
		return fmt.Errorf("must provide --cluster (you can create one via: `roachprod create $USER-bench -n 20 --gce-machine-type=n1-standard-8 --local-ssd=false`)")
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

	// Generate a unique build hash for the given targets and git revision.
	stdout, err := d.exec.CommandContextSilent(ctx, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	gitRevision := strings.TrimSpace(string(stdout))
	buildHash := md5.New()
	buildHash.Write([]byte(gitRevision))
	for _, target := range testTargets {
		buildHash.Write([]byte(target))
	}
	buildHashHex := hex.EncodeToString(buildHash.Sum(nil)[:])[:8]

	// Assume that we need LibGEOS.
	if err = d.buildGeos(ctx, volume); err != nil {
		return err
	}

	// Clear any old artifacts.
	outputPrefix := fmt.Sprintf("roachbench/%s", buildHashHex)
	binTar := fmt.Sprintf("artifacts/%s/bin.tar", outputPrefix)

	err = d.os.Remove(binTar)
	if err != nil {
		return err
	}
	err = d.os.Remove(binTar + ".gz")
	if err != nil {
		return err
	}

	// Build the test targets.
	var manifest strings.Builder
	crossTargets := testTargets
	for i := 0; i < len(crossTargets); i += batchSize {
		targetBatch := crossTargets[i:min(i+batchSize, len(crossTargets))]
		fmt.Printf("Building batch %v\n", targetBatch)

		// Build packages.
		crossArgs, targets, argErr := d.getBasicBuildArgs(ctx, targetBatch)
		if argErr != nil {
			return argErr
		}
		if race {
			crossArgs = append(crossArgs, "--config=race")
		}
		err = d.crossBuildTests(ctx, crossArgs, targets, "crosslinux", volume, outputPrefix)
		if err != nil {
			return err
		}

		// Add package name to the manifest file.
		for _, target := range targetBatch {
			targetDir := targetToDir(target)
			manifest.WriteString(fmt.Sprintf("%s\n", targetDir))
		}
	}

	if comress {
		log.Printf("Compressing output, this might take a while...")
		// Requires pigz (parallel gzip) to be installed on the system.
		compressionProgram := "pigz"
		err = d.ensureBinaryInPath(compressionProgram)
		if err != nil {
			log.Printf("Coud not find %s in PATH, falling back to gzip\n", compressionProgram)
			compressionProgram = "gzip"
		}
		stdout, err = d.exec.CommandContextSilent(ctx, compressionProgram, binTar)
		if err != nil {
			return err
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
	if _, cmdErr := d.exec.CommandContextSilent(ctx, "bazel", args...); cmdErr != nil {
		return cmdErr
	}
	if cmdErr := d.stageArtifacts(ctx, roachprodBenchTarget); cmdErr != nil {
		return cmdErr
	}

	libdir := path.Join(workspace, "artifacts", "libgeos", "lib")
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

	roachprodBenchArgs := []string{fmt.Sprintf("./artifacts/%s", outputPrefix), cluster, "-libdir", libdir}
	roachprodBenchArgs = append(roachprodBenchArgs, strings.Fields(benchArgs)...)
	roachprodBenchArgs = append(roachprodBenchArgs, "--")
	roachprodBenchArgs = append(roachprodBenchArgs, testArgs...)
	log.Printf("Running roachprod-bench with args: %s", strings.Join(roachprodBenchArgs, " "))
	return d.exec.CommandContextInheritingStdStreams(ctx, filepath.Join(workspace, "bin", "roachprod-bench"), roachprodBenchArgs...)
}

func (d *dev) crossBuildTests(
	ctx context.Context, bazelArgs []string, targets []buildTarget, crossConfig string, volume string, outputPrefix string,
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
	var bazelBinSet bool
	script.WriteString("set +x\n")
	for _, target := range targets {
		if target.kind == "go_binary" || target.kind == "go_test" {
			if !bazelBinSet {
				script.WriteString(fmt.Sprintf("BAZELBIN=$(bazel info bazel-bin %s)\n", shellescape.QuoteCommand(configArgs)))
				bazelBinSet = true
			}

			output := bazelutil.OutputOfBinaryRule(target.fullName, strings.Contains(crossConfig, "windows"))
			outputDir := fmt.Sprintf("$BAZELBIN/%s", output[:strings.LastIndex(output, "/")])
			tarDir := fmt.Sprintf("%s/bin/", targetToDir(target.fullName))
			targetDir := fmt.Sprintf("/artifacts/%s", outputPrefix)

			runfilesDirName := fmt.Sprintf("%s.runfiles", targetToBinBasename(target.fullName))

			baseOutput := filepath.Base(output)
			script.WriteString(fmt.Sprintf("mkdir -p %s\n", targetDir))

			// Setup runfiles environment variables that Bazel uses for execution.
			script.WriteString(fmt.Sprintf("echo \"export RUNFILES_DIR=\\$(pwd)/%s\" > %s/run.sh\n", runfilesDirName, outputDir))
			script.WriteString(fmt.Sprintf("echo \"export TEST_WORKSPACE=com_github_cockroachdb_cockroach/%s\" >> %s/run.sh\n",
				targetToDir(target.fullName), outputDir))

			// Add a soft link for testdata to support tests that use relative paths rather than the Bazel runfiles util.
			script.WriteString(fmt.Sprintf("echo \"find . -maxdepth 1 -type l -delete\" >> %s/run.sh\n", outputDir))
			script.WriteString(fmt.Sprintf("echo \"ln -f -s ./%s/com_github_cockroachdb_cockroach/%s/* .\" >> %s/run.sh\n",
				runfilesDirName, targetToDir(target.fullName), outputDir))
			script.WriteString(fmt.Sprintf("echo \"./%s \"\\$@\"\" >> %s/run.sh\n", targetToBinBasename(target.fullName), outputDir))
			script.WriteString(fmt.Sprintf("chmod +x %s/run.sh\n", outputDir))

			// Tar all test binaries and runfiles.
			script.WriteString(fmt.Sprintf("cd %s\n", outputDir))
			script.WriteString(fmt.Sprintf("tar --transform \"s#^./#%s#\" -hrf %s/bin.tar ./*\n", tarDir, targetDir))

			script.WriteString(fmt.Sprintf("echo \"Successfully built target %s at artifacts/%s\"\n", target.fullName, baseOutput))
		}
	}
	_, err = d.exec.CommandContextWithInput(ctx, script.String(), "docker", dockerArgs...)
	return err
}

func (d *dev) buildGeos(ctx context.Context, volume string) error {
	crossArgs, targets, err := d.getBasicBuildArgs(ctx, []string{"//c-deps:libgeos"})
	if err != nil {
		return err
	}
	return d.crossBuild(ctx, crossArgs, targets, "crosslinux", volume)
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func targetToDir(target string) string {
	return strings.TrimPrefix(target[:strings.LastIndex(target, ":")], "//")
}
