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
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/spf13/cobra"
)

const (
	doctorStatusFile = "bin/.dev-status"

	// doctorStatusVersion is the current "version" of the status checks
	// performed by `dev doctor``. Increasing it will force doctor to be re-run
	// before other dev commands can be run.
	doctorStatusVersion = 8

	noCacheFlag = "no-cache"
)

// getDoctorStatus returns the current doctor status number. This function only
// returns an error in exceptional situations -- if the status file does not
// already exist (as would be the case for a clean checkout), this function
// simply returns 0, nil.
func (d *dev) getDoctorStatus(ctx context.Context) (int, error) {
	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return -1, err
	}
	statusFile := filepath.Join(dir, doctorStatusFile)
	content, err := os.ReadFile(statusFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			content = []byte("0")
		} else {
			return -1, err
		}
	}
	return strconv.Atoi(strings.TrimSpace(string(content)))
}

// checkDoctorStatus returns an error iff the current doctor status is not the
// latest.
func (d *dev) checkDoctorStatus(ctx context.Context) error {
	if d.knobs.skipDoctorCheck {
		return nil
	}

	status, err := d.getDoctorStatus(ctx)
	if err != nil {
		return err
	}

	if status < doctorStatusVersion {
		return errors.New("please run `dev doctor` to refresh dev status, then try again")
	}
	return nil
}

func (d *dev) writeDoctorStatus(ctx context.Context) error {
	prevStatus, err := d.getDoctorStatus(ctx)
	if err != nil {
		return err
	}
	if prevStatus <= 0 {
		// In this case recommend the user `bazel clean --expunge`.
		log.Println("It is recommended to run `bazel clean --expunge` to avoid any spurious failures now that your machine is set up. (You only have to do this once.)")
	}
	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	statusFile := filepath.Join(dir, doctorStatusFile)

	return os.WriteFile(statusFile, []byte(strconv.Itoa(doctorStatusVersion)), 0600)
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

	log.Println("=============================")
	log.Println("=== RUNNING DOCTOR CHECKS ===")
	log.Println("=============================")

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

	d.log.Println("doctor: running node check")
	if runtime.GOOS == "freebsd" {
		// Having a pre-installed node is only necessary on freebsd.
		_, err := d.exec.CommandContextSilent(ctx, "/usr/local/bin/node", "--version")
		if err != nil {
			failures = append(failures, `/usr/local/bin/node not found.
You can install node with: `+"`pkg install node`")
		}
	}

	d.log.Println("doctor: running githooks check")
	{
		if _, err = d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--is-inside-work-tree"); err != nil {
			return err
		}
		stdout, err := d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--git-path", "hooks")
		if err != nil {
			return err
		}
		gitHooksDir := strings.TrimSpace(string(stdout))
		if err := d.os.RemoveAll(gitHooksDir); err != nil {
			return err
		}
		if err := d.os.MkdirAll(gitHooksDir); err != nil {
			return err
		}
		hooks, err := d.os.ListFilesWithSuffix("githooks", "")
		if err != nil {
			return err
		}
		for _, hook := range hooks {
			if err := d.os.Symlink(path.Join(workspace, hook), path.Join(gitHooksDir, path.Base(hook))); err != nil {
				return err
			}
		}
	}

	const binDir = "bin"
	const submodulesMarkerPath = binDir + "/.submodules-initialized"
	d.log.Println("doctor: running submodules check")
	if _, err := os.Stat(submodulesMarkerPath); errors.Is(err, os.ErrNotExist) {
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
	err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", "build", "//build/bazelutil:test_stamping")
	if err != nil {
		failedStampTestMsg = "Failed to run `bazel build //build/bazelutil:test_stamping`"
		log.Println(failedStampTestMsg)
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
Make sure one of the following lines is in the file %s/.bazelrc.user:
`, workspace)
		if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
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
	err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", "build", "//build/bazelutil:test_nogo_configured")
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
	}

	// Check whether the user has configured a custom tmpdir.
	present := d.checkLinePresenceInBazelRcUser(workspace, "test --test_tmpdir=")
	if !present {
		failures = append(failures, "You haven't configured a tmpdir for your tests.\n"+
			"Please add a `test --test_tmpdir=/PATH/TO/TMPDIR` line to your .bazelrc.user:\n"+
			fmt.Sprintf("    echo \"test --test_tmpdir=%s\" >> .bazelrc.user\n", filepath.Join(workspace, "tmp"))+
			"(You can choose any directory as a tmpdir.)")
	}

	// Make sure `patchelf` is installed if crosslinux config is used.
	d.log.Println("doctor: running patchelf check")
	if d.checkLinePresenceInBazelRcUser(workspace, "build --config=crosslinux") {
		_, err := d.exec.LookPath("patchelf")
		if err != nil {
			failures = append(failures, "patchelf not found on PATH. patchelf is required when using crosslinux config")
		}
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
	} else if !noCache {
		log.Println("doctor: skipping cache set up due to previous failures")
	}

	log.Println("======================================")
	log.Println("=== FINISHED RUNNING DOCTOR CHECKS ===")
	log.Println("======================================")

	if len(failures) > 0 {
		log.Printf("doctor: encountered %d errors", len(failures))
		for _, failure := range failures {
			log.Println(failure)
		}
		return errors.New("please address the errors described above and try again")
	}

	if err := d.writeDoctorStatus(ctx); err != nil {
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

// checkLinePresenceInBazelRcUser checks whether the .bazelrc.user file
// contains a line starting with the given prefix. Returns true iff a matching
// line is in the file. Failures to find the file are ignored.
func (d *dev) checkLinePresenceInBazelRcUser(workspace, expectedSubstr string) bool {
	contents, err := d.os.ReadFile(filepath.Join(workspace, ".bazelrc.user"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(contents, "\n") {
		if strings.HasPrefix(line, expectedSubstr) {
			return true
		}
	}
	return false
}
