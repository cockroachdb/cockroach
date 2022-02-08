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
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	stdos "os"
	stdexec "os/exec"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/os"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
	"github.com/irfansharif/recorder"
	"github.com/stretchr/testify/require"
)

// TestRecorderDriven makes use of datadriven to record (if --rewrite is
// specified) or play back (if --rewrite is omitted) all operations executed by
// individual `dev` invocations. The testcases are defined under
// testdata/recorderdriven/*; each test file corresponds to a captured recording
// found in testdata/recorderdriven/*.rec.
//
// DataDriven divvies up these files as subtests, so individual "files" are
// runnable through:
//
//  		dev test pkg/cmd/dev -f TestRecorderDriven/<fname>
// 	OR  go test ./pkg/cmd/dev -run TestRecorderDriven/<fname>
//
// Recordings are used to mock out "system" behavior. When --rewrite is
// specified, attempts to shell out to bazel or perform other OS operations
// (like creating, removing, symlinking filepaths) are intercepted and system
// responses are recorded for future playback. To update the test files with new
// capture data, try:
//
// 		go test ./pkg/cmd/dev -run TestRecorderDriven/<fname> -rewrite
//
// NB: This test is worth contrasting to TestDataDriven, where all operations
// are run in "dry-run" mode when --rewrite is specified. Here we'll actually
// shell out (and take time proportional to running the actual commands). In
// dry-run mode (TestDataDriven) all exec and os commands return successfully
// with no error, with an empty response. This makes it suitable for testing
// workflows that don't make use of external state to execute actions (like
// reading the set of targets from a file for e.g., or hoisting files from a
// sandbox by searching through the file system directly).
//
// TODO(irfansharif): When --rewrite-ing, because these tests shell out to the
// actual host system, it makes it difficult to run under bazel/dev (currently
// disallowed). Probably these tests should be ripped out entirely. Dev's
// currently in the business of doing a lot of interactive I/O with the host
// system, instead of pushing it all down into bazel rules. The recorder tests
// are the few remaining examples of this. As we push more things down into
// bazel rules, we should re-evaluate whether this harness provides much value.
// Probably dev commands that require writing a TestRecorderDriven test is worth
// re-writing.
//
func TestRecorderDriven(t *testing.T) {
	rewriting := false
	if f := flag.Lookup("rewrite"); f != nil && f.Value.String() == "true" {
		rewriting = true
	}
	if rewriting && bazel.BuiltWithBazel() {
		t.Fatalf("not supported under bazel") // needs to shell out to bazel itself
	}

	verbose := testing.Verbose()
	testdata := testutils.TestDataPath(t, "recorderdriven")
	datadriven.Walk(t, testdata, func(t *testing.T, path string) {
		if strings.HasSuffix(path, ".rec") {
			return
		}

		recordingPath := fmt.Sprintf("%s.rec", path)

		// We'll match against printed logs for datadriven.
		var logger io.ReadWriter = bytes.NewBufferString("")
		var recording io.ReadWriter
		var rec *recorder.Recorder

		execOpts := []exec.Option{exec.WithLogger(log.New(logger, "", 0))}
		osOpts := []os.Option{os.WithLogger(log.New(logger, "", 0))}

		if !verbose {
			// Suppress all internal output unless told otherwise.
			execOpts = append(execOpts, exec.WithStdOutErr(ioutil.Discard, ioutil.Discard))
		}

		if rewriting {
			workspaceResult := workspace(t)
			bazelbinResult := bazelbin(t)
			execOpts = append(execOpts,
				exec.WithWorkingDir(workspaceResult),
				exec.WithIntercept(workspaceCmd(), workspaceResult),
				exec.WithIntercept(bazelbinCmd(), bazelbinResult),
			)
			osOpts = append(osOpts, os.WithWorkingDir(workspaceResult))

			recording = bytes.NewBufferString("")
			rec = recorder.New(recorder.WithRecording(recording)) // the thing to record into
		} else {
			execOpts = append(execOpts,
				exec.WithIntercept(workspaceCmd(), crdbCheckoutPlaceholder),
				exec.WithIntercept(bazelbinCmd(), sandboxPlaceholder),
			)

			frecording, err := stdos.OpenFile(recordingPath, stdos.O_RDONLY, 0600)
			require.NoError(t, err)
			defer func() { require.NoError(t, frecording.Close()) }()
			rec = recorder.New(recorder.WithReplay(frecording, recordingPath)) // the recording we're playing back from
		}

		require.NotNil(t, rec)
		execOpts = append(execOpts, exec.WithRecorder(rec))
		osOpts = append(osOpts, os.WithRecorder(rec))

		devExec := exec.New(execOpts...)
		devOS := os.New(osOpts...)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			dev := makeDevCmd()
			dev.exec, dev.os = devExec, devOS
			dev.knobs.skipDoctorCheck = true
			dev.knobs.devBinOverride = "dev"

			if !verbose {
				dev.cli.SetErr(ioutil.Discard)
				dev.cli.SetOut(ioutil.Discard)
			}

			require.Equalf(t, d.Cmd, "dev", "unknown command: %s", d.Cmd)
			var args []string
			for _, cmdArg := range d.CmdArgs {
				args = append(args, cmdArg.Key)
				if len(cmdArg.Vals) != 0 {
					args = append(args, cmdArg.Vals[0])
				}
			}
			dev.cli.SetArgs(args)
			if err := dev.cli.Execute(); err != nil {
				return fmt.Sprintf("err: %s", err)
			}

			logs, err := ioutil.ReadAll(logger)
			require.NoError(t, err)
			if rewriting {
				logs = anonymize(t, logs)
			}
			return string(logs)
		})

		if rewriting {
			recording, err := ioutil.ReadAll(recording)
			require.NoError(t, err)

			frecording, err := stdos.OpenFile(recordingPath, stdos.O_CREATE|stdos.O_WRONLY|stdos.O_TRUNC|stdos.O_SYNC, 0600)
			require.NoError(t, err)
			defer func() { require.NoError(t, frecording.Close()) }()

			recording = anonymize(t, recording)
			_, err = frecording.Write(recording)
			require.NoError(t, err)
		}
	})
}

func anonymize(t *testing.T, input []byte) []byte {
	output := bytes.ReplaceAll(input, []byte(workspace(t)), []byte(crdbCheckoutPlaceholder))
	return bytes.ReplaceAll(output, []byte(bazelbin(t)), []byte(sandboxPlaceholder))
}

func workspace(t *testing.T) string {
	cmd := stdexec.Command("bazel", "info", "workspace")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait(), stderr.String())
	return strings.TrimSpace(stdout.String())
}

func bazelbin(t *testing.T) string {
	cmd := stdexec.Command("bazel", "info", "bazel-bin")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait(), stderr.String())
	return strings.TrimSpace(stdout.String())
}
