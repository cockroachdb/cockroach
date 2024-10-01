// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
)

const (
	outputFlag    = "output"
	batchSizeFlag = "batch-size"
	artifactsDir  = "artifacts"
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
ln -s #{.BazelOutputDir#}/#{.RunfilesDirName#} #{.StageDir#}/#{.RunfilesDirName#}
ln -s #{.BazelOutputDir#}/#{.TargetBinName#} #{.StageDir#}/#{.TargetBinName#}

echo #{shesc .RunScript#} > #{.StageDir#}/run.sh
chmod +x #{.StageDir#}/run.sh

echo "Successfully staged target #{.TargetName#}"
`

const packageTemplateScript = `
echo Packaging staged build output...
cd #{.ArtifactsDir#}
tar -chf bin.tar pkg

rm -rf pkg
bazel clean
`

const runTemplateScript = `
export RUNFILES_DIR=$(pwd)/#{.RunfilesDirName#}
export TEST_WORKSPACE=com_github_cockroachdb_cockroach/#{.PackageDir#}

find . -maxdepth 1 -type l -delete
ln -f -s ./#{.RunfilesDirName#}/com_github_cockroachdb_cockroach/#{.PackageDir#}/* .
./#{.TargetBinName#} "$@"
`

func makeTestBinariesCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	testBinariesCmd := &cobra.Command{
		Use:     "test-binaries <pkg>",
		Short:   "builds portable test binaries supplied with an executable run script for each package",
		Long:    "builds portable test binaries supplied with an executable run script for each package",
		Example: `dev test-binaries ./pkg/sql/... --output=test_binaries.tar.gz`,
		Args:    cobra.MinimumNArgs(1),
		RunE:    runE,
	}
	testBinariesCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory (only used for cross builds)")
	testBinariesCmd.Flags().StringArray(dockerArgsFlag, []string{}, "additional arguments to pass to Docker (only used for cross builds)")
	testBinariesCmd.Flags().String(outputFlag, "bin/test_binaries.tar.gz", "the file output path of the archived test binaries")
	testBinariesCmd.Flags().Bool(raceFlag, false, "produce race builds")
	testBinariesCmd.Flags().Int(batchSizeFlag, 128, "the number of packages to build per batch")
	testBinariesCmd.Flags().String(crossFlag, "crosslinux", "the cross build target to use")
	return testBinariesCmd
}

func (d *dev) testBinaries(cmd *cobra.Command, commandLine []string) error {
	ctx := cmd.Context()
	var (
		extraDockerArgs = mustGetFlagStringArray(cmd, dockerArgsFlag)
		volume          = mustGetFlagString(cmd, volumeFlag)
		output          = mustGetFlagString(cmd, outputFlag)
		race            = mustGetFlagBool(cmd, raceFlag)
		batchSize       = mustGetConstrainedFlagInt(cmd, batchSizeFlag, func(v int) error {
			if v <= 0 {
				return fmt.Errorf("%s must be greater than zero", batchSizeFlag)
			}
			return nil
		})
		crossConfig = mustGetFlagString(cmd, crossFlag)
	)

	packages, _ := splitArgsAtDash(cmd, commandLine)
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

	if output == "" {
		return fmt.Errorf("must provide --output")
	}

	compress := strings.HasSuffix(output, ".tar.gz")
	if !compress && !strings.HasSuffix(output, ".tar") {
		return fmt.Errorf("output must have the extension .tar or .tar.gz")
	}
	binTar := strings.TrimSuffix(output, ".gz")

	dockerArgs, err := d.getDockerRunArgs(ctx, volume, false, extraDockerArgs)
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
	}

	err = d.packageTestBinaries(ctx, dockerArgs, artifactsDir)
	if err != nil {
		return err
	}

	err = os.Rename(filepath.Join(artifactsDir, "bin.tar"), binTar)
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

	return nil
}

func (d *dev) crossBuildTests(
	ctx context.Context,
	dockerArgs []string,
	bazelArgs []string,
	targets []buildTarget,
	crossConfig string,
	artifactsDir string,
) error {
	bazelArgs = append(bazelArgs, fmt.Sprintf("--config=%s", crossConfig), "--crdb_test_off")
	configArgs := getConfigArgs(bazelArgs)
	stageOutputTemplate, err := createTemplate("stage", stageOutputTemplateScript)
	if err != nil {
		return err
	}
	runTemplate, err := createTemplate("run", runTemplateScript)
	if err != nil {
		return err
	}

	// Construct a script that builds the binaries in a temporary directory.
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

func (d *dev) packageTestBinaries(
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
