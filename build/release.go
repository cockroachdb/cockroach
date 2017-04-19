// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// This script builds a CockroachDB release binary, potentially cross compiling
// for a different platform. It must be run in the cockroachdb/builder docker
// image, as it depends on cross-compilation toolchains available there.
//
// Possible targets:
//   - linux-gnu:  target Linux 2.6.32, dynamically link glibc 2.12.2
//   - linux-musl: target Linux 2.6.32, statically link musl 1.1.16
//   - darwin:     target macOS 10.9
//   - windows:    target Windows 8, statically link all non-Windows libraries
//
// These targets must be kept in sync with the crosstool-ng toolchains installed
// in the Dockerfile.
//
// NB: This script forwards the GOFLAGS, SUFFIX, and TAGS environment variables
// as Make command-line variables to allow TeamCity to override those values as
// necessary.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

var releaseConfigurations = map[string]releaseConfiguration{
	"linux-gnu": {
		os:      "linux",
		arch:    "amd64",
		triple:  "x86_64-unknown-linux-gnu",
		ldflags: []string{"-static-libgcc", "-static-libstdc++"},
	},
	"linux-musl": {
		os:      "linux",
		arch:    "amd64",
		triple:  "x86_64-unknown-linux-musl",
		ldflags: []string{"-static"},
		suffix:  "-linux-2.6.32-musl-amd64",
	},
	"darwin": {
		os:     "darwin",
		arch:   "amd64",
		triple: "x86_64-apple-darwin13",
		suffix: "-darwin-10.9-amd64",
	},
	"windows": {
		os:     "windows",
		arch:   "amd64",
		triple: "x86_64-w64-mingw32",
		suffix: "-windows-6.2-amd64",
	},
}

var xtoolsBase = "/x-tools"

var defaultTarget = "build"

type releaseConfiguration struct {
	os      string
	arch    string
	triple  string
	ldflags []string
	suffix  string
}

func (rc *releaseConfiguration) linkflags() string {
	if len(rc.ldflags) > 0 {
		return fmt.Sprintf("-extldflags \"%s\"", strings.Join(rc.ldflags, " "))
	}
	return ""
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.Errorf("usage: go run release.go RELEASE-CONFIGURATION [MAKE-GOALS...]")
	}

	rc, ok := releaseConfigurations[os.Args[1]]
	if !ok {
		return errors.Errorf("fatal: unknown release configuration '%s'", os.Args[1])
	}

	sysroot := filepath.Join(xtoolsBase, rc.triple, "bin")
	if _, err := os.Stat(sysroot); os.IsNotExist(err) {
		return errors.Errorf("fatal: sysroot '%s' does not exist", sysroot)
	}
	if err := prependDirToPath(sysroot); err != nil {
		return err
	}

	if err := setEnv("CGO_ENABLED", "1"); err != nil {
		return err
	}

	args := []string{
		"TYPE=" + "release",
		"LINKFLAGS=" + rc.linkflags(),
		"GOFLAGS=" + os.Getenv("GOFLAGS"),
		"TAGS=" + os.Getenv("TAGS"),
		"SUFFIX=" + rc.suffix + os.Getenv("SUFFIX"),
		"TARGET_TRIPLE=" + rc.triple,
		"XCMAKE_SYSTEM_NAME=" + strings.Title(rc.os),
		"XGOOS=" + rc.os,
		"XGOARCH=" + rc.arch,
		"XCC=" + fmt.Sprintf("%s-cc", rc.triple),
		"XCXX=" + fmt.Sprintf("%s-c++", rc.triple),
	}
	args = append(args, os.Args[2:]...)
	return runMake(args)
}

func prependDirToPath(dir string) error {
	paths := append([]string{dir}, filepath.SplitList(os.Getenv("PATH"))...)
	return setEnv("PATH", strings.Join(paths, string(filepath.ListSeparator)))
}

func setEnv(key, value string) error {
	fmt.Fprintf(os.Stderr, "%s=%s\n", key, shellEscape(value))
	return os.Setenv(key, value)
}

func runMake(args []string) error {
	printArgs := []string{"make"}
	for _, arg := range args {
		printArgs = append(printArgs, shellEscape(arg))
	}
	fmt.Fprintln(os.Stderr, strings.Join(printArgs, " "))

	command := exec.Command("make", args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}

func shellEscape(s string) string {
	if strings.ContainsAny(s, " \t\n|&;()<>") {
		return fmt.Sprintf("\"%s\"", strings.Replace(s, "\"", "\\\"", -1))
	}
	return s
}
