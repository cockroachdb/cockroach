// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package release

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/errors"
)

// BuildOptions is a set of options that may be applied to a build.
type BuildOptions struct {
	// True iff this is a release build.
	Release bool
	// BuildTag must be set if Release is set, and vice-versea.
	BuildTag string

	// ExecFn.Run() is called to execute commands for this build.
	// The zero value is appropriate in "real" scenarios but for
	// tests you can update ExecFn.MockExecFn.
	ExecFn ExecFn
}

// SuffixFromPlatform returns the suffix that will be appended to the
// `cockroach` binary when built with the given platform. The binary
// itself can be found in pkgDir/cockroach$SUFFIX after the build.
func SuffixFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux:
		return ".linux-2.6.32-gnu-amd64"
	case PlatformLinuxFIPS:
		return ".linux-2.6.32-gnu-amd64-fips"
	case PlatformLinuxArm:
		return ".linux-3.7.10-gnu-arm64"
	case PlatformMacOS:
		// TODO(#release): The architecture is at least 10.10 until v20.2 and 10.15 for
		// v21.1 and after. Check whether this can be changed.
		return ".darwin-10.9-amd64"
	case PlatformMacOSArm:
		return ".darwin-11.0-arm64.unsigned"
	case PlatformWindows:
		return ".windows-6.2-amd64.exe"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// CrossConfigFromPlatform returns the cross*base config corresponding
// to the given platform. (See .bazelrc for more details.)
func CrossConfigFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux:
		return "crosslinuxbase"
	case PlatformLinuxFIPS:
		return "crosslinuxfipsbase"
	case PlatformLinuxArm:
		return "crosslinuxarmbase"
	case PlatformMacOS:
		return "crossmacosbase"
	case PlatformMacOSArm:
		return "crossmacosarmbase"
	case PlatformWindows:
		return "crosswindowsbase"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// TargetTripleFromPlatform returns the target triple that will be baked
// into the cockroach binary for the given platform.
func TargetTripleFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux, PlatformLinuxFIPS:
		return "x86_64-pc-linux-gnu"
	case PlatformLinuxArm:
		return "aarch64-unknown-linux-gnu"
	case PlatformMacOS:
		return "x86_64-apple-darwin19"
	case PlatformMacOSArm:
		return "aarch64-apple-darwin21.2"
	case PlatformWindows:
		return "x86_64-w64-mingw32"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// SharedLibraryExtensionFromPlatform returns the shared library extensions for a given Platform.
func SharedLibraryExtensionFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux, PlatformLinuxFIPS, PlatformLinuxArm:
		return ".so"
	case PlatformWindows:
		return ".dll"
	case PlatformMacOS, PlatformMacOSArm:
		return ".dylib"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// MakeWorkload makes the bin/workload binary. It is only ever built in the
// crosslinux configuration.
func MakeWorkload(opts BuildOptions, pkgDir string) error {
	if opts.Release {
		return errors.Newf("cannot build workload in Release mode")
	}
	// NB: workload doesn't need anything stamped so we can use `crosslinux`
	// rather than `crosslinuxbase`.
	cmd := exec.Command("bazel", "build", "//pkg/cmd/workload", "-c", "opt", "--config=crosslinux", "--config=ci")
	cmd.Dir = pkgDir
	cmd.Stderr = os.Stderr
	log.Printf("%s", cmd.Args)
	stdoutBytes, err := opts.ExecFn.Run(cmd)
	if err != nil {
		return errors.Wrapf(err, "failed to run %s: %s", cmd.Args, string(stdoutBytes))
	}

	bazelBin, err := getPathToBazelBin(opts.ExecFn, pkgDir, []string{"-c", "opt", "--config=crosslinux", "--config=ci"})
	if err != nil {
		return err
	}
	return stageBinary("//pkg/cmd/workload", PlatformLinux, bazelBin, filepath.Join(pkgDir, "bin"), false)
}

// MakeRelease makes the release binary and associated files.
func MakeRelease(platform Platform, opts BuildOptions, pkgDir string) error {
	buildArgs := []string{"build", "//pkg/cmd/cockroach", "//c-deps:libgeos", "//pkg/cmd/cockroach-sql"}
	targetTriple := TargetTripleFromPlatform(platform)
	if opts.Release {
		if opts.BuildTag == "" {
			return errors.Newf("must set BuildTag if Release is set")
		}
		buildArgs = append(buildArgs, fmt.Sprintf("--workspace_status_command=./build/bazelutil/stamp.sh %s official-binary %s release", targetTriple, opts.BuildTag))
	} else {
		if opts.BuildTag != "" {
			return errors.Newf("cannot set BuildTag if Release is not set")
		}
		buildArgs = append(buildArgs, fmt.Sprintf("--workspace_status_command=./build/bazelutil/stamp.sh %s official-binary", targetTriple))
	}
	configs := []string{"-c", "opt", "--config=ci", "--config=force_build_cdeps", "--config=with_ui", fmt.Sprintf("--config=%s", CrossConfigFromPlatform(platform))}
	buildArgs = append(buildArgs, configs...)
	cmd := exec.Command("bazel", buildArgs...)
	cmd.Dir = pkgDir
	cmd.Stderr = os.Stderr
	log.Printf("%s", cmd.Args)
	stdoutBytes, err := opts.ExecFn.Run(cmd)
	if err != nil {
		return errors.Wrapf(err, "failed to run %s: %s", cmd.Args, string(stdoutBytes))
	}

	// Stage binaries from bazel-bin.
	bazelBin, err := getPathToBazelBin(opts.ExecFn, pkgDir, configs)
	if err != nil {
		return err
	}
	if err := stageBinary("//pkg/cmd/cockroach", platform, bazelBin, pkgDir, true); err != nil {
		return err
	}
	// TODO: strip the bianry
	if err := stageBinary("//pkg/cmd/cockroach-sql", platform, bazelBin, pkgDir, true); err != nil {
		return err
	}
	if err := stageLibraries(platform, bazelBin, filepath.Join(pkgDir, "lib")); err != nil {
		return err
	}

	if platform == PlatformLinux || platform == PlatformLinuxFIPS {
		suffix := SuffixFromPlatform(platform)
		binaryName := "./cockroach" + suffix

		cmd := exec.Command(binaryName, "version")
		cmd.Dir = pkgDir
		cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		stdoutBytes, err := opts.ExecFn.Run(cmd)
		if err != nil {
			return errors.Wrapf(err, "%s %s: %s", cmd.Env, cmd.Args, string(stdoutBytes))
		}

		cmd = exec.Command("ldd", binaryName)
		cmd.Dir = pkgDir
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		stdoutBytes, err = opts.ExecFn.Run(cmd)
		if err != nil {
			log.Fatalf("%s %s: out=%s err=%v", cmd.Env, cmd.Args, string(stdoutBytes), err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(stdoutBytes))
		for scanner.Scan() {
			if line := scanner.Text(); !linuxStaticLibsRe.MatchString(line) {
				return errors.Newf("%s is not properly statically linked:\n%s", binaryName, line)
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		cmd = exec.Command("bazel", "run", "@go_sdk//:bin/go", "--", "tool", "nm", binaryName)
		cmd.Dir = pkgDir
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		stdoutBytes, err = opts.ExecFn.Run(cmd)
		if err != nil {
			log.Fatalf("%s %s: out=%s err=%v", cmd.Env, cmd.Args, string(stdoutBytes), err)
		}
		out := string(stdoutBytes)
		if platform == PlatformLinuxFIPS && !strings.Contains(out, "golang-fips") {
			log.Print("`go nm tool` does not contain `golang-fips` in its output")
			log.Fatalf("%s %s: out=%s", cmd.Env, cmd.Args, out)
		}
		if platform == PlatformLinux && strings.Contains(out, "golang-fips") {
			log.Print("`go nm tool` contains `golang-fips` in its output")
			log.Fatalf("%s %s: out=%s", cmd.Env, cmd.Args, out)
		}
	}
	return nil
}

var (
	// linuxStaticLibsRe returns the regexp of all static libraries.
	linuxStaticLibsRe = func() *regexp.Regexp {
		libs := strings.Join([]string{
			regexp.QuoteMeta("linux-vdso.so."),
			regexp.QuoteMeta("librt.so."),
			regexp.QuoteMeta("libpthread.so."),
			regexp.QuoteMeta("libdl.so."),
			regexp.QuoteMeta("libm.so."),
			regexp.QuoteMeta("libc.so."),
			regexp.QuoteMeta("libresolv.so."),
			strings.Replace(regexp.QuoteMeta("ld-linux-ARCH.so."), "ARCH", ".*", -1),
		}, "|")
		return regexp.MustCompile(libs)
	}()
	osVersionRe = regexp.MustCompile(`\d+\.(\d+(\.)?)*?-`)
)

// Platform is an enumeration of the supported platforms for release.
type Platform int

const (
	// PlatformLinux is the Linux x86_64 target.
	PlatformLinux Platform = iota
	// PlatformLinuxFIPS is the Linux FIPS target.
	PlatformLinuxFIPS
	// PlatformLinuxArm is the Linux aarch64 target.
	PlatformLinuxArm
	// PlatformMacOS is the Darwin x86_64 target.
	PlatformMacOS
	// PlatformMacOSArm is the Darwin aarch6 target.
	PlatformMacOSArm
	// PlatformWindows is the Windows (mingw) x86_64 target.
	PlatformWindows
)

func getPathToBazelBin(execFn ExecFn, pkgDir string, configArgs []string) (string, error) {
	args := []string{"info", "bazel-bin"}
	args = append(args, configArgs...)
	cmd := exec.Command("bazel", args...)
	cmd.Dir = pkgDir
	cmd.Stderr = os.Stderr
	stdoutBytes, err := execFn.Run(cmd)
	if err != nil {
		return "", errors.Wrapf(err, "failed to run %s: %s", cmd.Args, string(stdoutBytes))
	}
	return strings.TrimSpace(string(stdoutBytes)), nil
}

func stageBinary(
	target string, platform Platform, bazelBin string, dir string, includePlatformSuffix bool,
) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	rel := util.OutputOfBinaryRule(target, platform == PlatformWindows)
	src := filepath.Join(bazelBin, rel)
	dstBase, _ := TrimDotExe(filepath.Base(rel))
	suffix := ""
	if includePlatformSuffix {
		suffix = SuffixFromPlatform(platform)
	}
	dstBase = dstBase + suffix
	dst := filepath.Join(dir, dstBase)
	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	defer closeFileOrPanic(srcF)
	dstF, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer closeFileOrPanic(dstF)
	_, err = io.Copy(dstF, srcF)
	return err
}

func stageLibraries(platform Platform, bazelBin string, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	ext := SharedLibraryExtensionFromPlatform(platform)
	for _, lib := range CRDBSharedLibraries {
		libDir := "lib"
		if platform == PlatformWindows {
			// NB: On Windows these libs end up in the `bin` subdir.
			libDir = "bin"
		}
		src := filepath.Join(bazelBin, "c-deps", "libgeos_foreign", libDir, lib+ext)
		srcF, err := os.Open(src)
		if err != nil {
			return err
		}
		defer closeFileOrPanic(srcF)
		dst := filepath.Join(dir, filepath.Base(src))
		dstF, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer closeFileOrPanic(dstF)
		_, err = io.Copy(dstF, srcF)
		if err != nil {
			return err
		}
	}
	return nil
}

func closeFileOrPanic(f io.Closer) {
	err := f.Close()
	if err != nil {
		panic(errors.Wrapf(err, "could not close file"))
	}
}
