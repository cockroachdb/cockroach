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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/spf13/cobra"
)

const (
	doctorStatusFile = "bin/.dev-status"

	// doctorStatusVersion is the current "version" of the status checks performed
	// by `dev doctor``. Increasing it will force doctor to be re-run before other
	// dev commands can be run.
	doctorStatusVersion = 3

	noCacheFlag = "no-cache"
)

func (d *dev) checkDoctorStatus(ctx context.Context) error {
	if d.knobs.skipDoctorCheck {
		return nil
	}

	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	statusFile := filepath.Join(dir, doctorStatusFile)
	content, err := ioutil.ReadFile(statusFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			content = []byte("0")
		} else {
			return err
		}
	}
	status, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil {
		return err
	}
	if status < doctorStatusVersion {
		return errors.New("please run `dev doctor` to refresh dev status, then try again")
	}
	return nil
}

func (d *dev) writeDoctorStatus(ctx context.Context, ex *exec.Exec) error {
	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	statusFile := filepath.Join(dir, doctorStatusFile)

	return ioutil.WriteFile(statusFile, []byte(strconv.Itoa(doctorStatusVersion)), 0600)
}

func printStdoutAndErr(stdoutStr string, err error) {
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
}

// makeDoctorCmd constructs the subcommand used to build the specified binaries.
func makeDoctorCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "doctor",
		Aliases: []string{"setup"},
		Short:   "Check whether your machine is ready to build",
		Long:    "Check whether your machine is ready to build.",
		Example: "dev doctor",
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
	cmd.Flags().Bool(noCacheFlag, false, "do not set up remote cache as part of doctor")
	return cmd
}

func (d *dev) doctor(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	failures := []string{}
	noCache := mustGetFlagBool(cmd, noCacheFlag)
	noCacheEnv := d.os.Getenv("DEV_NO_REMOTE_CACHE")
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	if noCacheEnv != "" {
		noCache = true
	}

	// If we're running on macOS, we need to check whether XCode is installed.
	d.log.Println("doctor: running xcode check")
	if runtime.GOOS == "darwin" {
		stdout, err := d.exec.CommandContextSilent(ctx, "/usr/bin/xcodebuild", "-version")
		if err != nil {
			log.Println("Failed to run `/usr/bin/xcodebuild -version`.")
			stdoutStr := strings.TrimSpace(string(stdout))
			printStdoutAndErr(stdoutStr, err)
			failures = append(failures, `You must have a full installation of XCode to build with Bazel.
A command-line tools instance does not suffice.
Please perform the following steps:
  1. Install XCode from the App Store.
  2. Launch Xcode.app at least once to perform one-time initialization of developer tools.
  3. Run `+"`xcode-select -switch /Applications/Xcode.app/`.")
		}
	}

	const cmakeRequiredMajor, cmakeRequiredMinor = 3, 20
	d.log.Println("doctor: running cmake check")
	{
		stdout, err := d.exec.CommandContextSilent(ctx, "cmake", "--version")
		stdoutStr := strings.TrimSpace(string(stdout))
		if err != nil {
			log.Println("Failed to run `cmake --version`.")
			printStdoutAndErr(stdoutStr, err)
			failures = append(failures, "Failed to run `cmake --version`; do you have it installed?")
		} else {
			versionFields := strings.Split(strings.TrimPrefix(stdoutStr, "cmake version "), ".")
			if len(versionFields) < 3 {
				failures = append(failures, fmt.Sprintf("malformed cmake version:   %q", stdoutStr))
			} else {
				major, majorErr := strconv.Atoi(versionFields[0])
				minor, minorErr := strconv.Atoi(versionFields[1])
				if majorErr != nil || minorErr != nil {
					failures = append(failures, fmt.Sprintf("malformed cmake version:   %q", stdoutStr))
				} else if major < cmakeRequiredMajor || minor < cmakeRequiredMinor {
					msg := "cmake is too old, upgrade to 3.20.x+"
					if runtime.GOOS == "linux" {
						msg = msg + "\n\t If this is a gceworker you can use ./build/bootstrap/bootstrap-debian.sh to update all tools"
					}
					failures = append(failures, msg)
				}
			}
		}
	}

	const binDir = "bin"
	const submodulesMarkerPath = binDir + "/.submodules-initialized"
	d.log.Println("doctor: running submodules check")
	if _, err := os.Stat(submodulesMarkerPath); errors.Is(err, os.ErrNotExist) {
		if _, err = d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--is-inside-work-tree"); err != nil {
			return err
		}
		if _, err = d.exec.CommandContextSilent(ctx, "git", "submodule", "update", "--init", "--recursive"); err != nil {
			return err
		}
		if err = d.os.MkdirAll(binDir); err != nil {
			return err
		}
		if err = d.os.WriteFile(submodulesMarkerPath, ""); err != nil {
			return err
		}
	}

	// Check whether the build is properly configured to use stamping.
	d.log.Println("doctor: running stamp test")
	failedStampTestMsg := ""
	stdout, err := d.exec.CommandContextSilent(ctx, "bazel", "build", "//build/bazelutil:test_stamping")
	if err != nil {
		failedStampTestMsg = "Failed to run `bazel build //build/bazelutil:test_stamping`"
		log.Println(failedStampTestMsg)
		printStdoutAndErr(string(stdout), err)
	} else {
		bazelBin, err := d.getBazelBin(ctx)
		if err != nil {
			return err
		}
		testStampingTxt := filepath.Join(bazelBin, "build", "bazelutil", "test_stamping.txt")
		fileContents, err := d.os.ReadFile(testStampingTxt)
		if err != nil {
			return err
		}
		if !strings.Contains(fileContents, "STABLE_BUILD_TYPE") {
			failedStampTestMsg = fmt.Sprintf("Could not find STABLE_BUILD_TYPE in %s\n", testStampingTxt)
		}
	}
	if failedStampTestMsg != "" {
		failedStampTestMsg = failedStampTestMsg + fmt.Sprintf(`
This may be because your Bazel is not configured to "stamp" built executables.
Make sure one of the following lines is in the file %s/.bazelrc.user:
`, workspace)
		if runtime.GOOS == "darwin" && runtime.GOARCH == "amd64" {
			failedStampTestMsg = failedStampTestMsg + "    build --config=devdarwinx86_64"
		} else if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
			failedStampTestMsg = failedStampTestMsg + "    build --config=dev\n"
			failedStampTestMsg = failedStampTestMsg + "             OR       \n"
			failedStampTestMsg = failedStampTestMsg + "    build --config=crosslinux\n"
			failedStampTestMsg = failedStampTestMsg + "The former will use your host toolchain, while the latter will use the cross-compiler that we use in CI."
		} else {
			failedStampTestMsg = failedStampTestMsg + "    build --config=dev"
		}
		failures = append(failures, failedStampTestMsg)
	}

	// Check whether linting during builds (nogo) is explicitly configured
	// before we get started.
	stdout, err = d.exec.CommandContextSilent(ctx, "bazel", "build", "//build/bazelutil:test_nogo_configured")
	if err != nil {
		failedNogoTestMsg := "Failed to run `bazel build //build/bazelutil:test_nogo_configured. " + `
This may be because you haven't configured whether to run lints during builds.
Put EXACTLY ONE of the following lines in your .bazelrc.user:
    build --config lintonbuild
        OR
    build --config nolintonbuild
The former will run lint checks while you build. This will make incremental builds
slightly slower and introduce a noticeable delay in first-time build setup.`
		failures = append(failures, failedNogoTestMsg)
		log.Println(failedNogoTestMsg)
		printStdoutAndErr(string(stdout), err)
	}

	// We want to make sure there are no other failures before trying to
	// set up the cache.
	if !noCache && len(failures) == 0 {
		d.log.Println("doctor: setting up cache")
		bazelRcLine, err := d.setUpCache(ctx)
		if err != nil {
			return err
		}
		msg, err := d.checkPresenceInBazelRc(bazelRcLine)
		if err != nil {
			return err
		}
		if msg != "" {
			failures = append(failures, msg)
		}
	}

	if len(failures) > 0 {
		log.Printf("doctor: encountered %d errors", len(failures))
		for _, failure := range failures {
			log.Println(failure)
		}
		return errors.New("please address the errors described above and try again")
	}

	if err := d.writeDoctorStatus(ctx, d.exec); err != nil {
		return err
	}
	log.Println("You are ready to build :)")
	return nil
}

// checkPresenceInBazelRc checks whether the given line is in ~/.bazelrc.
// If it is, this function returns an empty string and a nil error.
// If it isn't, this function returns a non-empty human-readable string describing
// what the user should do to solve the issue and a nil error.
// In other failure cases the function returns an empty string and a non-nil error.
func (d *dev) checkPresenceInBazelRc(expectedBazelRcLine string) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	errString := fmt.Sprintf("Please add the string `%s` to your ~/.bazelrc:\n", expectedBazelRcLine)
	errString = errString + fmt.Sprintf("    echo \"%s\" >> ~/.bazelrc", expectedBazelRcLine)

	bazelRcContents, err := d.os.ReadFile(filepath.Join(homeDir, ".bazelrc"))
	if err != nil {
		// The file may not exist; that's OK, but the line definitely is
		// not in the file.
		return errString, nil //nolint:returnerrcheck
	}
	found := false
	for _, line := range strings.Split(bazelRcContents, "\n") {
		if !strings.Contains(line, expectedBazelRcLine) {
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		found = true
	}
	if found {
		return "", nil
	}
	return errString, nil
}
