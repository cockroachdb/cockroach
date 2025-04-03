// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build bazel

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
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
	buildSubcmd             = "build"
	runSubcmd               = "run"
	testSubcmd              = "test"
	coverageSubcmd          = "coverage"
	mergeTestXMLsSubcmd     = "merge-test-xmls"
	mungeTestXMLSubcmd      = "munge-test-xml"
	beaverHubServerEndpoint = "https://beaver-hub-server-jjd2v2r2dq-uk.a.run.app/process"
)

type builtArtifact struct {
	src, dst string
}

type fullTestResult struct {
	run, shard, attempt int32
	testResult          *bes.TestResult
}

var (
	port                    int
	artifactsDir            string
	githubPostFormatterName string

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
	testXmls         []string
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
				if summary != nil && summary.ShardCount > 1 {
					outputDir = filepath.Join(outputDir, fmt.Sprintf("shard_%d_of_%d", testResult.shard, summary.ShardCount))
				}
				// Add `.tc_ignore_attempt#` to the filename of all attempts but the
				// last one. This ensures that those results are uploaded to TC in case
				// we need them but the results are ignored by TC because the filename
				// doesn't end with `.xml`.
				append_tc_ignore := summary != nil && testResult.attempt != summary.AttemptCount
				if testResult.testResult == nil {
					continue
				}
				for _, output := range testResult.testResult.TestActionOutput {
					if output.Name == "test.log" || output.Name == "test.xml" || output.Name == "test.lcov" {
						src := strings.TrimPrefix(output.GetUri(), "file://")
						dst := filepath.Join(artifactsDir, outputDir, filepath.Base(src))
						if append_tc_ignore {
							dst += fmt.Sprintf(".tc_ignore_%d", int(testResult.attempt))
						}
						if err := doCopy(src, dst); err != nil {
							return nil, err
						}
						if output.Name == "test.xml" {
							s.testXmls = append(s.testXmls, src)
						}
					} else {
						panic(fmt.Sprintf("Unknown TestActionOutput: %v", output))
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
			if target == nil {
				continue
			}
			for _, outputGroup := range target.OutputGroup {
				if outputGroup == nil || outputGroup.Incomplete {
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
	rootCmd.Flags().IntVar(
		&port,
		"port",
		8998,
		"port to run the bazci server on",
	)
}

func getRunEnvForBeaverHub() string {
	branch := strings.TrimPrefix(os.Getenv("TC_BUILD_BRANCH"), "refs/heads/")
	if strings.Contains(branch, "master") || strings.Contains(branch, "release") || strings.Contains(branch, "staging") {
		return branch
	}
	return "PR"
}

func sendBepDataToBeaverHub(bepFilepath string) error {
	file, err := os.Open(bepFilepath)
	if err != nil {
		return err
	}
	defer file.Close()
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", beaverHubServerEndpoint, file)
	req.Header.Add("Run-Env", getRunEnvForBeaverHub())
	req.Header.Add("Content-Type", "application/octet-stream")
	if _, err := httpClient.Do(req); err != nil {
		return err
	}
	return nil
}

func bazciImpl(cmd *cobra.Command, args []string) error {
	if args[0] != buildSubcmd && args[0] != runSubcmd && args[0] != coverageSubcmd &&
		args[0] != testSubcmd && args[0] != mungeTestXMLSubcmd && args[0] != mergeTestXMLsSubcmd {
		return errors.Newf("First argument must be `build`, `run`, `test`, `coverage`, `merge-test-xmls`, or `munge-test-xml`; got %v", args[0])
	}

	// Special case: munge-test-xml/merge-test-xmls don't require running Bazel at all.
	// Perform the munge then exit immediately.
	if args[0] == mungeTestXMLSubcmd {
		return mungeTestXMLs(args)
	}
	if args[0] == mergeTestXMLsSubcmd {
		return mergeTestXMLs(args)
	}

	server := newMonitorBuildServer(args[0])
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	bepLoc := filepath.Join(tmpDir, "beplog")
	if err := server.Start(); err != nil {
		return err
	}
	args = append(args, fmt.Sprintf("--build_event_binary_file=%s", bepLoc))
	args = append(args, fmt.Sprintf("--bes_backend=grpc://127.0.0.1:%d", port))
	// Insert `--config=ci` if it's not already in the args list,
	// specifically for tests.
	if args[0] == testSubcmd || args[0] == coverageSubcmd {
		hasCiConfig := false
		for idx, arg := range args {
			if arg == "--config=ci" || arg == "--config=cinolint" ||
				(arg == "--config" && idx < len(args)-1 && (args[idx+1] == "ci" || args[idx+1] == "cinolint")) {
				hasCiConfig = true
				break
			}
		}
		if !hasCiConfig {
			args = append(args, "--config", "ci")
		}
	}
	fmt.Println("running bazel w/ args: ", shellescape.QuoteCommand(args))
	bazelCmd := exec.Command("bazel", args...)
	var stdout, stderr bytes.Buffer
	bazelCmd.Stdout = io.MultiWriter(os.Stdout, bufio.NewWriter(&stdout))
	bazelCmd.Stderr = io.MultiWriter(os.Stderr, bufio.NewWriter(&stderr))
	bazelErr := bazelCmd.Run()
	if bazelErr != nil {
		fmt.Printf("got error %+v from bazel run\n", bazelErr)
	}
	server.Wait()
	if err := sendBepDataToBeaverHub(bepLoc); err != nil {
		// Retry.
		if err := sendBepDataToBeaverHub(bepLoc); err != nil {
			fmt.Printf("Sending BEP data to beaver hub failed - %v\n", err)
		}
	}

	removeEmergencyBallasts()
	// Presumably a build failure.
	if bazelErr != nil && len(server.testXmls) == 0 {
		postBuildFailure(fmt.Sprintf("stdout: %s\n, stderr: %s", stdout.String(), stderr.String()))
		return bazelErr
	}

	return errors.CombineErrors(processTestXmls(server.testXmls), bazelErr)
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

// Some unit tests test automatic ballast creation. These ballasts can be
// larger than the maximum artifact size. Remove any artifacts with the
// EMERGENCY_BALLAST filename.
func removeEmergencyBallasts() {
	findCmdArgs := []string{
		artifactsDir,
		"-name",
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

func processTestXmls(testXmls []string) error {
	if doPost() {
		var postErrors []string
		for _, testXml := range testXmls {
			xmlFile, err := os.ReadFile(testXml)
			if err != nil {
				postErrors = append(postErrors, fmt.Sprintf("Failed to read %s with the following error: %v", testXml, err))
				continue
			}
			var testSuites bazelutil.TestSuites
			err = xml.Unmarshal(xmlFile, &testSuites)
			if err != nil {
				postErrors = append(postErrors, fmt.Sprintf("Failed to parse test.xml file with the following error: %+v", err))
				continue
			}
			if err := githubpost.PostFromTestXMLWithFormatterName(githubPostFormatterName, testSuites); err != nil {
				postErrors = append(postErrors, fmt.Sprintf("Failed to process %s with the following error: %+v", testXml, err))
				continue
			}
		}
		if len(postErrors) != 0 {
			return errors.Newf("%s", strings.Join(postErrors, "\n"))
		}
	}
	return nil
}

func postBuildFailure(logs string) {
	if doPost() {
		githubpost.PostGeneralFailure(githubPostFormatterName, logs)
	}
}

func doPost() bool {
	branch := strings.TrimPrefix(os.Getenv("TC_BUILD_BRANCH"), "refs/heads/")
	isReleaseBranch := strings.HasPrefix(branch, "master") || strings.HasPrefix(branch, "release") || strings.HasPrefix(branch, "provisional")
	if !isReleaseBranch {
		fmt.Printf("branch %s does not appear to be a release branch; skipping reporting issues to GitHub\n", branch)
		return false
	}
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
		fmt.Println("GITHUB_API_TOKEN must be set to post results to GitHub; skipping error reporting")
		// TODO(ricky): Certain jobs (nightlies) probably really
		// do need to fail outright in this case rather than
		// silently continuing here. How do we handle them?
		return false
	}
	return true
}
