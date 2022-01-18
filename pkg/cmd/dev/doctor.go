// Copyright 2020 The Cockroach Authors.
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
	"errors"
	"log"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

// makeDoctorCmd constructs the subcommand used to build the specified binaries.
func makeDoctorCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	return &cobra.Command{
		Use:     "doctor",
		Short:   "Check whether your machine is ready to build",
		Long:    "Check whether your machine is ready to build.",
		Example: "dev doctor",
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
}

func (d *dev) doctor(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	success := true

	// If we're running on macOS, we need to check whether XCode is installed.
	d.log.Println("doctor: running xcode check")
	if runtime.GOOS == "darwin" {
		stdout, err := d.exec.CommandContextSilent(ctx, "/usr/bin/xcodebuild", "-version")
		if err != nil {
			success = false
			log.Printf("Failed to run `/usr/bin/xcodebuild -version`.")
			stdoutStr := strings.TrimSpace(string(stdout))
			if len(stdoutStr) > 0 {
				log.Printf("stdout:   %s", stdoutStr)
			}
			var cmderr *exec.ExitError
			if errors.As(err, &cmderr) {
				stderrStr := strings.TrimSpace(string(cmderr.Stderr))
				if len(stderrStr) > 0 {
					log.Printf("stderr:   %s", stderrStr)
				}
			}
			log.Println(`You must have a full installation of XCode to build with Bazel.
A command-line tools instance does not suffice.
Please perform the following steps:
  1. Install XCode from the App Store.
  2. Launch Xcode.app at least once to perform one-time initialization of developer tools.
  3. Run ` + "`xcode-select -switch /Applications/Xcode.app/`.")
		}
	}

	// Check whether the build is properly configured to use stamping.
	passedStampTest := true
	if _, err := d.exec.CommandContextSilent(ctx, "bazel", "build", "//build/bazelutil:test_stamping"); err != nil {
		passedStampTest = false
	} else {
		bazelBin, err := d.getBazelBin(ctx)
		if err != nil {
			return err
		}
		fileContents, err := d.os.ReadFile(
			filepath.Join(bazelBin, "build", "bazelutil", "test_stamping.txt"))
		if err != nil {
			return err
		}
		if !strings.Contains(fileContents, "STABLE_BUILD_GIT_COMMIT") {
			passedStampTest = false
		}
	}
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	if !passedStampTest {
		success = false
		log.Printf(`Your machine is not configured to "stamp" your built executables.
Please add one of the following to your %s/.bazelrc.user:`, workspace)
		if runtime.GOOS == "darwin" && runtime.GOARCH == "amd64" {
			log.Printf("    build --config=devdarwinx86_64")
		} else if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
			log.Printf("    build --config=dev")
			log.Printf("             OR       ")
			log.Printf("    build --config=crosslinux")
			log.Printf("The former will use your host toolchain, while the latter will use the cross-compiler that we use in CI.")
		} else {
			log.Printf("    build --config=dev")
		}
	}

	if success {
		log.Println("You are ready to build :)")
		return nil
	}
	return errors.New("please address the errors described above and try again")
}
