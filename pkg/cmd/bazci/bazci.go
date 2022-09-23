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
	artifactsDir string

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
		"path where artifacts should be staged")
}

func bazciImpl(cmd *cobra.Command, args []string) error {
	if args[0] != buildSubcmd && args[0] != runSubcmd && args[0] != testSubcmd && args[0] != mungeTestXMLSubcmd && args[0] != mergeTestXMLsSubcmd {
		return errors.Newf("First argument must be `build`, `run`, `test`, `merge-test-xmls`, or `munge-test-xml`; got %v", args[0])
	}

	// Special case: munge-test-xml/merge-test-xmls don't require running Bazel at all.
	// Perform the munge then exit immediately.
	if args[0] == mungeTestXMLSubcmd {
		return mungeTestXMLs(args)
	}
	if args[0] == mergeTestXMLsSubcmd {
		return mergeTestXMLs(args)
	}

	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	bepLoc := filepath.Join(tmpDir, "beplog")
	args = append(args, fmt.Sprintf("--build_event_binary_file=%s", bepLoc))
	fmt.Println("running bazel w/ args: ", shellescape.QuoteCommand(args))
	bazelCmd := exec.Command("bazel", args...)
	bazelCmd.Stdout = os.Stdout
	bazelCmd.Stderr = os.Stderr
	err = bazelCmd.Run()
	if err != nil {
		fmt.Printf("got error %+v from bazel run\n", err)
		fmt.Println("WARNING: the beplog file may not have been created")
	}

	return processBuildEventProtocolLog(args[0], bepLoc)
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
