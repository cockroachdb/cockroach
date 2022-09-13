// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build bazel
// +build bazel

package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/alessio/shellescape"
	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/testfilter"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
)

const (
	buildSubcmd         = "build"
	runSubcmd           = "run"
	testSubcmd          = "test"
	mergeTestXMLsSubcmd = "merge-test-xmls"
	mungeTestXMLSubcmd  = "munge-test-xml"
)

type builtArtifact struct {
	src, dst string
}

type fullTestResult struct {
	run, shard, attempt int32
	testResult          *bes.TestResult
}

var (
	artifactsDir              string
	githubPostFormatterName   string
	shouldProcessTestFailures bool

	rootCmd = &cobra.Command{
		Use:   "bazci",
		Short: "A glue binary for making Bazel usable in Teamcity",
		Long: `bazci is glue code to make debugging Bazel builds and
tests in Teamcity as painless as possible.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          bazciImpl,
	}
)

func init() {
	rootCmd.Flags().StringVar(
		&artifactsDir,
		"artifacts_dir",
		"/artifacts",
		"path where artifacts should be staged",
	)
	rootCmd.Flags().StringVar(
		&githubPostFormatterName,
		"formatter",
		"default",
		"formatter name for githubpost",
	)
	rootCmd.Flags().BoolVar(
		&shouldProcessTestFailures,
		"process_test_failures",
		false,
		"process failures artifacts (and post github issues for release failures)",
	)
}

func bazciImpl(cmd *cobra.Command, args []string) (retErr error) {
	var goTestJSONOutputFilePath string
	defer func() {
		if err := processTestJSONIfNeeded(retErr != nil /* shouldCreateTarball */, goTestJSONOutputFilePath); err != nil {
			fmt.Printf("failed to process go test json output - %v\n", err)
		}
	}()

	if args[0] != buildSubcmd && args[0] != runSubcmd && args[0] != testSubcmd && args[0] != mungeTestXMLSubcmd && args[0] != mergeTestXMLsSubcmd {
		retErr = errors.Newf("First argument must be `build`, `run`, `test`, `merge-test-xmls`, or `munge-test-xml`; got %v", args[0])
		return
	}

	// Special case: munge-test-xml/merge-test-xmls don't require running Bazel at all.
	// Perform the munge then exit immediately.
	if args[0] == mungeTestXMLSubcmd {
		retErr = mungeTestXMLs(args)
		return
	}
	if args[0] == mergeTestXMLsSubcmd {
		retErr = mergeTestXMLs(args)
		return
	}

	tmpDir, retErr := os.MkdirTemp("", "")
	if retErr != nil {
		return
	}
	bepLoc := filepath.Join(tmpDir, "beplog")
	args = append(args, fmt.Sprintf("--build_event_binary_file=%s", bepLoc))
	if shouldProcessTestFailures {
		f, createTempErr := os.CreateTemp(artifactsDir, "test.json.txt")
		if createTempErr != nil {
			retErr = createTempErr
			return
		}
		goTestJSONOutputFilePath = f.Name()
		// Closing the file because we will not use the file pointer.
		if retErr = f.Close(); retErr != nil {
			return
		}
		args = append(args, "--test_env", goTestJSONOutputFilePath)
	}

	fmt.Println("running bazel w/ args: ", shellescape.QuoteCommand(args))
	bazelCmd := exec.Command("bazel", args...)
	bazelCmd.Stdout = os.Stdout
	bazelCmd.Stderr = os.Stderr
	bazelCmdErr := bazelCmd.Run()
	if bazelCmdErr != nil {
		fmt.Printf("got error %+v from bazel run\n", bazelCmdErr)
		fmt.Println("WARNING: the beplog file may not have been created")
	}
	retErr = processBuildEventProtocolLog(args[0], bepLoc)
	return
}

func mungeTestXMLs(args []string) error {
	for _, file := range args[1:] {
		contents, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		err = bazelutil.MungeTestXML(contents, &buf)
		if err != nil {
			return err
		}
		err = os.WriteFile(file, buf.Bytes(), 0666)
		if err != nil {
			return err
		}
	}
	return nil
}

func mergeTestXMLs(args []string) error {
	var xmlsToMerge []bazelutil.TestSuites
	for _, file := range args[1:] {
		contents, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		var testSuites bazelutil.TestSuites
		err = xml.Unmarshal(contents, &testSuites)
		if err != nil {
			return err
		}
		xmlsToMerge = append(xmlsToMerge, testSuites)
	}
	return bazelutil.MergeTestXMLs(xmlsToMerge, os.Stdout)
}

func processBuildEventProtocolLog(action, bepLoc string) error {
	contents, err := os.ReadFile(bepLoc)
	if err != nil {
		return err
	}
	namedSetsOfFiles := make(map[string][]builtArtifact)
	testResults := make(map[string][]fullTestResult)
	builtTargets := make(map[string]*bes.TargetComplete)
	buf := proto.NewBuffer(contents)
	var event bes.BuildEvent
	for {
		err := buf.DecodeMessage(&event)
		if err != nil {
			return err
		}
		// Capture the output binaries/files.
		switch id := event.Id.Id.(type) {
		case *bes.BuildEventId_NamedSet:
			namedSetID := id.NamedSet.Id
			namedSet := event.GetNamedSetOfFiles()
			var files []builtArtifact
			for _, file := range namedSet.Files {
				uri := file.GetUri()
				files = append(files, builtArtifact{src: strings.TrimPrefix(uri, "file://"), dst: file.Name})
			}
			for _, set := range namedSet.FileSets {
				files = append(files, namedSetsOfFiles[set.Id]...)
			}
			namedSetsOfFiles[namedSetID] = files
		case *bes.BuildEventId_TestResult:
			res := fullTestResult{
				run:        id.TestResult.Run,
				shard:      id.TestResult.Shard,
				attempt:    id.TestResult.Attempt,
				testResult: event.GetTestResult(),
			}
			testResults[id.TestResult.Label] = append(testResults[id.TestResult.Label], res)
		case *bes.BuildEventId_TargetCompleted:
			builtTargets[id.TargetCompleted.Label] = event.GetCompleted()
		case *bes.BuildEventId_TestSummary:
			label := id.TestSummary.Label
			outputDir := strings.TrimPrefix(label, "//")
			outputDir = strings.ReplaceAll(outputDir, ":", "/")
			outputDir = filepath.Join("bazel-testlogs", outputDir)
			summary := event.GetTestSummary()
			for _, testResult := range testResults[label] {
				outputDir := outputDir
				if testResult.run > 1 {
					outputDir = filepath.Join(outputDir, fmt.Sprintf("run_%d", testResult.run))
				}
				if summary.ShardCount > 1 {
					outputDir = filepath.Join(outputDir, fmt.Sprintf("shard_%d_of_%d", testResult.shard, summary.ShardCount))
				}
				if testResult.attempt > 1 {
					outputDir = filepath.Join(outputDir, fmt.Sprintf("attempt_%d", testResult.attempt))
				}
				for _, output := range testResult.testResult.TestActionOutput {
					if output.Name == "test.log" || output.Name == "test.xml" {
						src := strings.TrimPrefix(output.GetUri(), "file://")
						dst := filepath.Join(artifactsDir, outputDir, filepath.Base(src))
						if err := doCopy(src, dst); err != nil {
							return err
						}
					} else {
						panic(output)
					}
				}
			}
		}

		if event.LastMessage {
			break
		}
	}

	if action == "build" {
		for _, target := range builtTargets {
			for _, outputGroup := range target.OutputGroup {
				if outputGroup.Incomplete {
					continue
				}
				for _, set := range outputGroup.FileSets {
					for _, artifact := range namedSetsOfFiles[set.Id] {
						if err := doCopy(artifact.src, filepath.Join(artifactsDir, "bazel-bin", artifact.dst)); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

// doCopy copies from the src file to the destination. Note that we will
// also munge the schema of the src file if it is a test.xml.
func doCopy(src, dst string) error {
	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	srcStat, err := srcF.Stat()
	if err != nil {
		return err
	}
	dstMode := 0666
	if srcStat.Mode()&0111 != 0 {
		dstMode = 0777
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
		return err
	}
	dstF, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(dstMode))
	if err != nil {
		return err
	}
	if filepath.Base(src) != "test.xml" {
		_, err = io.Copy(dstF, srcF)
	} else {
		var srcContent []byte
		srcContent, err = io.ReadAll(srcF)
		if err != nil {
			return err
		}
		err = bazelutil.MungeTestXML(srcContent, dstF)
	}
	return err
}

func processFailures(goTestJSONOutputFileBuf []byte, failuresFilePath string) error {
	pr, pw := io.Pipe()
	err := testfilter.FilterAndWrite(bytes.NewReader(goTestJSONOutputFileBuf), pw, []string{"strip", "omit", "convert"})
	if err != nil {
		return err
	}
	f, err := os.Create(failuresFilePath)
	if err != nil {
		return err
	}
	written, err := io.Copy(f, pr)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if written == 0 {
		if err := os.Remove(failuresFilePath); err != nil {
			return err
		}
	}
	return nil
}

func postReleaseOnlyFailures(goTestJSONOutputFileBuf []byte) error {
	branch := strings.TrimPrefix(os.Getenv("TC_BUILD_BRANCH"), "refs/heads/")
	isReleaseBranch := strings.HasPrefix(branch, "master") || strings.HasPrefix(branch, "release") || strings.HasPrefix(branch, "provisional")
	if isReleaseBranch {
		// GITHUB_API_TOKEN must be in the env or github-post will barf if it's
		// ever asked to post, so enforce that on all runs.
		// The way this env var is made available here is quite tricky. The build
		// calling this method is usually a build that is invoked from PRs, so it
		// can't have secrets available to it (for the PR could modify
		// build/teamcity-* to leak the secret). Instead, we provide the secrets
		// to a higher-level job (Publish Bleeding Edge) and use TeamCity magic to
		// pass that env var through when it's there. This means we won't have the
		// env var on PR builds, but we'll have it for builds that are triggered
		// from the release branches.
		if os.Getenv("GITHUB_API_TOKEN") == "" {
			return errors.New("GITHUB_API_TOKEN must be set")
		}
		githubpost.Post(githubPostFormatterName, bytes.NewReader(goTestJSONOutputFileBuf))
	}
	return nil
}

// createTarball converts the test json output file into output intended for human eyes
// and creates a tarball that contains the original json file and the converted
// human-readable file.
func createTarball(goTestJSONOutputFilePath string) error {
	buf, err := os.ReadFile(goTestJSONOutputFilePath)
	if err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(artifactsDir, "full_output.txt"))
	if err != nil {
		return err
	}
	if err := testfilter.FilterAndWrite(bytes.NewReader(buf), f, []string{"convert"}); err != nil {
		return err
	}

	tarArgs := []string{
		"--strip-components=1",
		"-czf",
		"full_output.tgz",
		"full_output.txt",
		filepath.Base(goTestJSONOutputFilePath),
	}
	createTarballCmd := exec.Command("tar", tarArgs...)
	createTarballCmd.Dir = artifactsDir
	var errBuf bytes.Buffer
	createTarballCmd.Stderr = &errBuf
	fmt.Println("running tar w/ args: ", shellescape.QuoteCommand(tarArgs))
	if err := createTarballCmd.Run(); err != nil {
		return errors.Wrapf(err, "StdErr: %s", errBuf.String())
	}
	if err := os.Remove(filepath.Join(artifactsDir, "full_output.txt")); err != nil {
		fmt.Printf("Failed to remove full_output.txt - %v\n", err)
	}
	return nil
}

// Some unit tests test automatic ballast creation. These ballasts can be
// larger than the maximum artifact size. Remove any artifacts with the
// EMERGENCY_BALLAST filename.
func removeEmergencyBallasts() {
	findCmdArgs := []string{
		"-name",
		artifactsDir,
		"EMERGENCY_BALLAST",
		"-delete",
	}
	findCmd := exec.Command("find", findCmdArgs...)
	var errBuf bytes.Buffer
	findCmd.Stderr = &errBuf
	if err := findCmd.Run(); err != nil {
		fmt.Println("running find w/ args: ", shellescape.QuoteCommand(findCmdArgs))
		fmt.Printf("Failed with err %v\nStdErr: %v", err, errBuf.String())
	}
}

func processTestJSONIfNeeded(shouldCreateTarball bool, goTestJSONOutputFilePath string) error {
	if !shouldProcessTestFailures {
		return nil
	}
	removeEmergencyBallasts()
	buf, err := os.ReadFile(goTestJSONOutputFilePath)
	if err != nil {
		return err
	}
	if err := processFailures(buf, filepath.Join(artifactsDir, "failures.txt")); err != nil {
		return err
	}
	if err := postReleaseOnlyFailures(buf); err != nil {
		return err
	}
	if shouldCreateTarball {
		if err := createTarball(goTestJSONOutputFilePath); err != nil {
			return err
		}
	}
	return nil
}
