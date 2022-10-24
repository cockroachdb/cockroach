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
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/alessio/shellescape"
	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/testfilter"
	"github.com/cockroachdb/errors"
	ptypes "github.com/gogo/protobuf/types"
	"github.com/spf13/cobra"
	// We're using auto-generated protos here, not building our own. We can
	// switch to building our own if there appears to be an advantage (maybe
	// there is if we use our own grpc client?)
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
	port                      int
	artifactsDir              string
	shouldProcessTestFailures bool
	githubPostFormatterName   string

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

type monitorBuildServer struct {
	action           string // "build" or "test".
	namedSetsOfFiles map[string][]builtArtifact
	testResults      map[string][]fullTestResult
	builtTargets     map[string]*bes.TargetComplete
	// Send a bool value to this channel when it's time to tear down the
	// server.
	finished chan bool
	wg       sync.WaitGroup
}

func newMonitorBuildServer(action string) *monitorBuildServer {
	return &monitorBuildServer{
		action:           action,
		namedSetsOfFiles: make(map[string][]builtArtifact),
		testResults:      make(map[string][]fullTestResult),
		builtTargets:     make(map[string]*bes.TargetComplete),
		finished:         make(chan bool, 1),
	}
}

func (s *monitorBuildServer) Start() error {
	conn, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	build.RegisterPublishBuildEventServer(srv, s)
	s.wg.Add(2)
	go func() {
		// This worker gracefully stops the server when the build is finished.
		defer s.wg.Done()
		<-s.finished
		srv.GracefulStop()
	}()
	go func() {
		// This worker runs the server.
		defer s.wg.Done()
		if err := srv.Serve(conn); err != nil {
			panic(err)
		}
	}()
	return nil
}

func (s *monitorBuildServer) Wait() {
	s.wg.Wait()
}

// This function is needed to implement PublishBuildEventServer. We just use the
// BuildEvent_BuildFinished event to tell us when the server needs to be torn
// down.
func (s *monitorBuildServer) PublishLifecycleEvent(
	ctx context.Context, req *build.PublishLifecycleEventRequest,
) (*emptypb.Empty, error) {
	switch req.BuildEvent.Event.Event.(type) {
	case *build.BuildEvent_BuildFinished_:
		s.finished <- true
	}
	return &emptypb.Empty{}, nil
}

func (s *monitorBuildServer) PublishBuildToolEventStream(
	stream build.PublishBuildEvent_PublishBuildToolEventStreamServer,
) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		response, err := s.handleBuildEvent(stream.Context(), in)
		if err != nil {
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return s.Finalize()
}

// Handle the given build event and return an appropriate response.
func (s *monitorBuildServer) handleBuildEvent(
	ctx context.Context, in *build.PublishBuildToolEventStreamRequest,
) (*build.PublishBuildToolEventStreamResponse, error) {
	switch event := in.OrderedBuildEvent.Event.Event.(type) {
	case *build.BuildEvent_BazelEvent:
		var bazelBuildEvent bes.BuildEvent
		any := &ptypes.Any{
			TypeUrl: event.BazelEvent.TypeUrl,
			Value:   event.BazelEvent.Value,
		}
		if err := ptypes.UnmarshalAny(any, &bazelBuildEvent); err != nil {
			return nil, err
		}
		switch id := bazelBuildEvent.Id.Id.(type) {
		case *bes.BuildEventId_NamedSet:
			namedSetID := id.NamedSet.Id
			namedSet := bazelBuildEvent.GetNamedSetOfFiles()
			var files []builtArtifact
			for _, file := range namedSet.Files {
				uri := file.GetUri()
				files = append(files, builtArtifact{src: strings.TrimPrefix(uri, "file://"), dst: file.Name})
			}
			for _, set := range namedSet.FileSets {
				files = append(files, s.namedSetsOfFiles[set.Id]...)
			}
			s.namedSetsOfFiles[namedSetID] = files
		case *bes.BuildEventId_TestResult:
			res := fullTestResult{
				run:        id.TestResult.Run,
				shard:      id.TestResult.Shard,
				attempt:    id.TestResult.Attempt,
				testResult: bazelBuildEvent.GetTestResult(),
			}
			s.testResults[id.TestResult.Label] = append(s.testResults[id.TestResult.Label], res)
		case *bes.BuildEventId_TargetCompleted:
			s.builtTargets[id.TargetCompleted.Label] = bazelBuildEvent.GetCompleted()
		case *bes.BuildEventId_TestSummary:
			label := id.TestSummary.Label
			outputDir := strings.TrimPrefix(label, "//")
			outputDir = strings.ReplaceAll(outputDir, ":", "/")
			outputDir = filepath.Join("bazel-testlogs", outputDir)
			summary := bazelBuildEvent.GetTestSummary()
			for _, testResult := range s.testResults[label] {
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
							return nil, err
						}
					} else {
						panic(output)
					}
				}
			}
		}
	}

	return &build.PublishBuildToolEventStreamResponse{
		StreamId:       in.OrderedBuildEvent.StreamId,
		SequenceNumber: in.OrderedBuildEvent.SequenceNumber,
	}, nil
}

func (s *monitorBuildServer) Finalize() error {
	if s.action == "build" {
		for _, target := range s.builtTargets {
			for _, outputGroup := range target.OutputGroup {
				if outputGroup.Incomplete {
					continue
				}
				for _, set := range outputGroup.FileSets {
					for _, artifact := range s.namedSetsOfFiles[set.Id] {
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
	rootCmd.Flags().IntVar(
		&port,
		"port",
		8998,
		"port to run the bazci server on",
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

	server := newMonitorBuildServer(args[0])
	if err := server.Start(); err != nil {
		retErr = err
		return
	}
	args = append(args, fmt.Sprintf("--bes_backend=grpc://127.0.0.1:%d", port))
	fmt.Println("running bazel w/ args: ", shellescape.QuoteCommand(args))
	bazelCmd := exec.Command("bazel", args...)
	bazelCmd.Stdout = os.Stdout
	bazelCmd.Stderr = os.Stderr
	retErr = bazelCmd.Run()
	if retErr != nil {
		fmt.Printf("got error %+v from bazel run\n", retErr)
	}
	server.Wait()
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
