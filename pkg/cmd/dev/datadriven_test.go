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
	"fmt"
	"io"
	"io/ioutil"
	"log"
	stdos "os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/os"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recording"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func init() {
	isTesting = true
}

// TestDatadriven makes use of datadriven to play back all operations executed
// by individual `dev` invocations. The testcases are defined under testdata/*,
// where each test files corresponds to a recording capture found in
// testdata/recording/*.
//
// Datadriven divvies up these files as subtests, so individual "files" are
// runnable through:
//
// 		go test -run TestDatadriven/<fname>
//
// Recordings are used to mock out "system" behavior. During these test runs
// (unless -record is specified), attempts to shell out to `bazel` or perform
// other OS operations are intercepted and responses are constructed using
// recorded data.
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
		var exopts []exec.Option
		var osopts []os.Option

		exopts = append(exopts, exec.WithLogger(log.New(logger, "", 0)))
		osopts = append(osopts, os.WithLogger(log.New(logger, "", 0)))

		if !verbose {
			// Suppress all internal output unless told otherwise.
			exopts = append(exopts, exec.WithStdOutErr(ioutil.Discard, ioutil.Discard))
		}

		frecording, err := stdos.OpenFile(recordingPath, stdos.O_RDONLY, 0600)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, frecording.Close())
		}()

		r := recording.WithReplayFrom(frecording, recordingPath)
		exopts = append(exopts, exec.WithRecording(r))
		osopts = append(osopts, os.WithRecording(r))

		devExec := exec.New(exopts...)
		devOS := os.New(osopts...)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			dev := makeDevCmd()
			dev.exec = devExec
			dev.os = devOS
			require.NoError(t, setupPath(dev))

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
	})
}
