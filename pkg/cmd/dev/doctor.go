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
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/spf13/cobra"
)

const doctorStatusFile = "bin/.dev-status"

// doctorStatusVersion is the current "version" of the status checks performed
// by `dev doctor``. Increasing it will force doctor to be re-run before other
// dev commands can be run.
const doctorStatusVersion = 1

func (d *dev) checkDoctorStatus(ctx context.Context) error {
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
	var cmderr *osexec.ExitError
	if errors.As(err, &cmderr) {
		stderrStr := strings.TrimSpace(string(cmderr.Stderr))
		if len(stderrStr) > 0 {
			log.Printf("stderr:   %s", stderrStr)
		}
	}
}

// makeDoctorCmd constructs the subcommand used to build the specified binaries.
func makeDoctorCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	return &cobra.Command{
		Use:     "doctor",
		Aliases: []string{"setup"},
		Short:   "Check whether your machine is ready to build",
		Long:    "Check whether your machine is ready to build.",
		Example: "dev doctor",
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
}

func (d *dev) doctor(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	if err := d.setupGithooks(ctx); err != nil {
		return err
	}
	success, err := d.checkRequiredTools(ctx)
	if err != nil {
		return err
	}
	if !success {
		return errors.New("please address the errors described above and try again")
	}
	if err := d.writeDoctorStatus(ctx, d.exec); err != nil {
		return err
	}
	log.Println("You are ready to build :)")
	return nil
}

func (d *dev) setupGithooks(ctx context.Context) error {
	hookdirStdout, err := d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--git-path", "hooks")
	if err != nil {
		return err
	}
	hooksDir, err := filepath.Abs(strings.TrimSpace(string(hookdirStdout)))
	if err != nil {
		return err
	}
	if err := os.Mkdir(hooksDir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}

	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	srcHooks := filepath.Join(dir, "githooks")

	return filepath.Walk(srcHooks, func(s string, i fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			return nil
		}
		link := filepath.Join(hooksDir, filepath.Base(s))
		if _, err := os.Stat(link); err == nil {
			return nil
		}

		target, err := filepath.Rel(hooksDir, s)
		if err != nil {
			return err
		}
		log.Printf("linking %s to %s", link, target)
		os.Symlink(target, link)
		return nil
	})
}

func (d *dev) checkRequiredTools(ctx context.Context) (bool, error) {
	success := true
	// If we're running on macOS, we need to check whether XCode is installed.
	d.log.Println("doctor: running xcode check")
	if runtime.GOOS == "darwin" {
		stdout, err := d.exec.CommandContextSilent(ctx, "/usr/bin/xcodebuild", "-version")
		if err != nil {
			success = false
			log.Printf("Failed to run `/usr/bin/xcodebuild -version`.")
			stdoutStr := strings.TrimSpace(string(stdout))
			printStdoutAndErr(stdoutStr, err)
			log.Println(`You must have a full installation of XCode to build with Bazel.
A command-line tools instance does not suffice.
Please perform the following steps:
  1. Install XCode from the App Store.
  2. Launch Xcode.app at least once to perform one-time initialization of developer tools.
  3. Run ` + "`xcode-select -switch /Applications/Xcode.app/`.")
		}
	}

	const cmakeRequiredMajor, cmakeRequiredMinor = 3, 20
	d.log.Println("doctor: running cmake check")
	{
		stdout, err := d.exec.CommandContextSilent(ctx, "cmake", "--version")
		stdoutStr := strings.TrimSpace(string(stdout))
		if err != nil {
			printStdoutAndErr(stdoutStr, err)
			success = false
		} else {
			versionFields := strings.Split(strings.TrimPrefix(stdoutStr, "cmake version "), ".")
			if len(versionFields) < 3 {
				log.Printf("malformed cmake version:   %q\n", stdoutStr)
				success = false
			} else {
				major, majorErr := strconv.Atoi(versionFields[0])
				minor, minorErr := strconv.Atoi(versionFields[1])
				if majorErr != nil || minorErr != nil {
					log.Printf("malformed cmake version:   %q\n", stdoutStr)
					success = false
				} else if major < cmakeRequiredMajor || minor < cmakeRequiredMinor {
					log.Printf("cmake is too old, upgrade to 3.20.x+\n")
					if runtime.GOOS == "linux" {
						log.Printf("\t If this is a gceworker you can use ./build/bootstrap/bootstrap-debian.sh to update all tools\n")
					}
					success = false
				}
			}
		}
	}

	const binDir = "bin"
	const submodulesMarkerPath = binDir + "/.submodules-initialized"
	d.log.Println("doctor: running submodules check")
	if _, err := os.Stat(submodulesMarkerPath); errors.Is(err, os.ErrNotExist) {
		if _, err = d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--is-inside-work-tree"); err != nil {
			return false, err
		}
		if _, err = d.exec.CommandContextSilent(ctx, "git", "submodule", "update", "--init", "--recursive"); err != nil {
			return false, err
		}
		if err = d.os.MkdirAll(binDir); err != nil {
			return false, err
		}
		if err = d.os.WriteFile(submodulesMarkerPath, ""); err != nil {
			return false, err
		}
	}

	// Check whether the build is properly configured to use stamping.
	passedStampTest := true
	if _, err := d.exec.CommandContextSilent(ctx, "bazel", "build", "//build/bazelutil:test_stamping"); err != nil {
		passedStampTest = false
	} else {
		bazelBin, err := d.getBazelBin(ctx)
		if err != nil {
			return false, err
		}
		fileContents, err := d.os.ReadFile(
			filepath.Join(bazelBin, "build", "bazelutil", "test_stamping.txt"))
		if err != nil {
			return false, err
		}
		if !strings.Contains(fileContents, "STABLE_BUILD_GIT_BUILD_TYPE") {
			passedStampTest = false
		}
	}
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return false, err
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
	return success, nil
}
