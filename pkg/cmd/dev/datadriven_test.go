// Copyright 2021 The Cockroach Authors.
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
	"go/build"
	"io"
	"io/ioutil"
	"log"
	stdos "os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/os"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recorder"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

var (
	recordFlag = flag.Bool(
		"record", false,
		"ignore existing recordings and rewrite them with results from an actual execution (see -from-checkout)",
	)

	fromCheckoutFlag = flag.String(
		"from-checkout", crdbpath(),
		"cockroach/cockroachdb checkout to generate recordings from",
	)
)

// TestDatadriven makes use of datadriven and recorder to capture all operations
// executed by individual `dev` invocations. The testcases are defined under
// testdata/*, where each test files corresponds to a recording capture found in
// testdata/recording/*.
//
// Datadriven divvies up these files as subtests, so individual "files" are
// runnable through:
//
// 		go test -run TestDatadriven/<fname>
//
// To update the test file with new capture data, try:
//
// 		go test -run TestDatadriven/<fname> -rewrite -record \
// 			[-from-checkout=<path to crdb checkout>]
//
// The -rewrite flag rewrites what is found under testdata/<fname>, while the
// -record flag rewrites what is found under testdata/recording/<fname>.
// Recordings are used to mock out "system" behavior. During these test runs
// (unless -record is specified), attempts to shell out to `bazel` are
// intercepted and responses are constructed using recorded data. The same is
// true for attempts to run os operations (like creating/removing/symlinking
// filepaths).
//
// In summary: the traces for all operations attempted as part of test run are
// captured within testdata/<fname> and the mocked out responses for each
// "external" command can be found under testadata/recording/<fname>. The former
// is updated by specifying -rewrite, the latter is updated by specifying
// -record (and optionally -from-checkout).
func TestDatadriven(t *testing.T) {
	verbose := testing.Verbose()
	testdata := testutils.TestDataPath(t)
	datadriven.Walk(t, testdata, func(t *testing.T, path string) {
		if strings.HasPrefix(path, filepath.Join(testdata, "recording")) {
			return
		}

		dir, file := filepath.Split(path)
		recordingPath := filepath.Join(dir, "recording", file) // path to the recording, if any

		// We'll match against printed logs for datadriven.
		var logger io.ReadWriter = bytes.NewBufferString("")
		var recording io.ReadWriter
		var exopts []exec.Option
		var osopts []os.Option

		exopts = append(exopts, exec.WithLogger(log.New(logger, "", 0)))
		osopts = append(osopts, os.WithLogger(log.New(logger, "", 0)))

		if !verbose {
			// Suppress all internal output unless told otherwise.
			exopts = append(exopts, exec.WithStdOutErr(ioutil.Discard, ioutil.Discard))
		}

		if *recordFlag {
			recording = bytes.NewBufferString("")

			r := recorder.New(recorder.WithRecordingTo(recording))          // the thing to record into
			exopts = append(exopts, exec.WithWorkingDir(*fromCheckoutFlag)) // the path to run dev from
			osopts = append(osopts, os.WithWorkingDir(*fromCheckoutFlag))   // the path to run dev from
			exopts = append(exopts, exec.WithRecorder(r))
			osopts = append(osopts, os.WithRecorder(r))
		} else {
			frecording, err := stdos.OpenFile(recordingPath, stdos.O_RDONLY, 0600)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, frecording.Close())
			}()

			r := recorder.New(recorder.WithReplayFrom(frecording, recordingPath)) // the recording we're playing back from
			exopts = append(exopts, exec.WithRecorder(r))
			osopts = append(osopts, os.WithRecorder(r))
		}

		devExec := exec.New(exopts...)
		devOS := os.New(osopts...)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			dev := makeDevCmd()
			dev.exec = devExec
			dev.os = devOS

			if !verbose {
				dev.cli.SetErr(ioutil.Discard)
				dev.cli.SetOut(ioutil.Discard)
			}

			switch d.Cmd {
			case "dev":
				var args []string
				for _, cmdArg := range d.CmdArgs {
					args = append(args, cmdArg.Key)
					if len(cmdArg.Vals) != 0 {
						args = append(args, cmdArg.Vals[0])
					}
				}
				dev.cli.SetArgs(args)
				require.NoError(t, dev.cli.Execute())

				logs, err := ioutil.ReadAll(logger)
				require.NoError(t, err)

				return fmt.Sprintf("%s", logs)
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})

		if *recordFlag {
			recording, err := ioutil.ReadAll(recording)
			require.NoError(t, err)

			frecording, err := stdos.OpenFile(recordingPath, stdos.O_CREATE|stdos.O_WRONLY|stdos.O_TRUNC|stdos.O_SYNC, 0600)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, frecording.Close())
			}()

			_, err = frecording.Write(recording)
			require.NoError(t, err)
		}
	})
}

func crdbpath() string {
	gopath := stdos.Getenv("GOPATH")
	if gopath == "" {
		gopath = build.Default.GOPATH
	}
	return filepath.Join(gopath, "src", "github.com", "cockroachdb", "cockroach")
}
