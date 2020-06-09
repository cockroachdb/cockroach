// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package release contains utilities for assisting with the release process.
// This is intended for use for the release commands.
package release

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// linuxStaticLibsRe returns the regexp of all static libraries.
var linuxStaticLibsRe = func() *regexp.Regexp {
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

// SupportedTarget contains metadata about a supported target.
type SupportedTarget struct {
	BuildType string
	Suffix    string
}

// SupportedTargets contains the supported targets that we build.
var SupportedTargets = []SupportedTarget{
	{BuildType: "darwin", Suffix: ".darwin-10.9-amd64"},
	{BuildType: "linux-gnu", Suffix: ".linux-2.6.32-gnu-amd64"},
	{BuildType: "windows", Suffix: ".windows-6.2-amd64.exe"},
}

// makeReleaseAndVerifyOptions are options for MakeRelease.
type makeReleaseAndVerifyOptions struct {
	args   []string
	execFn ExecFn
}

// ExecFn is a mockable wrapper that executes the given command.
type ExecFn func(*exec.Cmd) ([]byte, error)

// DefaultExecFn is the default exec function.
var DefaultExecFn ExecFn = func(c *exec.Cmd) ([]byte, error) {
	if c.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	var stdout bytes.Buffer
	c.Stdout = io.MultiWriter(&stdout, os.Stdout)
	err := c.Run()
	return stdout.Bytes(), err
}

// MakeReleaseOption as an option for the MakeRelease function.
type MakeReleaseOption func(makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions

// WithMakeReleaseOptionBuildArg adds a build argument to release.
func WithMakeReleaseOptionBuildArg(arg string) MakeReleaseOption {
	return func(m makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions {
		m.args = append(m.args, arg)
		return m
	}
}

// WithMakeReleaseOptionExecFn changes the exec function of the given execFn.
func WithMakeReleaseOptionExecFn(r ExecFn) MakeReleaseOption {
	return func(m makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions {
		m.execFn = r
		return m
	}
}

// MakeRelease makes the release binary.
func MakeRelease(b SupportedTarget, pkgDir string, opts ...MakeReleaseOption) error {
	params := makeReleaseAndVerifyOptions{
		execFn: DefaultExecFn,
	}
	for _, opt := range opts {
		params = opt(params)
	}

	{
		args := append(
			[]string{b.BuildType, fmt.Sprintf("%s=%s", "SUFFIX", b.Suffix)},
			params.args...,
		)
		cmd := exec.Command("mkrelease", args...)
		cmd.Dir = pkgDir
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if out, err := params.execFn(cmd); err != nil {
			return errors.Newf("%s %s: %s\n\n%s", cmd.Env, cmd.Args, err, out)
		}
	}
	if strings.Contains(b.BuildType, "linux") {
		binaryName := "./cockroach" + b.Suffix

		cmd := exec.Command(binaryName, "version")
		cmd.Dir = pkgDir
		cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if out, err := params.execFn(cmd); err != nil {
			return errors.Newf("%s %s: %s\n\n%s", cmd.Env, cmd.Args, err, out)
		}

		cmd = exec.Command("ldd", binaryName)
		cmd.Dir = pkgDir
		log.Printf("%s %s", cmd.Env, cmd.Args)
		out, err := params.execFn(cmd)
		if err != nil {
			log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			if line := scanner.Text(); !linuxStaticLibsRe.MatchString(line) {
				return errors.Newf("%s is not properly statically linked:\n%s", binaryName, out)
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}
