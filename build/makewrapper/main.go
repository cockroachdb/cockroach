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

const xtoolsBase = "/x-tools"

type releaseConfiguration struct {
	os      string
	arch    string
	triple  string
	ldflags []string
	suffix  string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.Errorf("usage: %s RELEASE-CONFIGURATION [MAKE-GOALS...]", os.Args[0])
	}

	rc, ok := releaseConfigurations[os.Args[1]]
	if !ok {
		return errors.Errorf("unknown release configuration '%s'", os.Args[1])
	}

	args := []string{
		"TYPE=" + "release",
		"TARGET_TRIPLE=" + rc.triple,
		"XCMAKE_SYSTEM_NAME=" + strings.Title(rc.os),
		"XGOOS=" + rc.os,
		"XGOARCH=" + rc.arch,
		"XCC=" + fmt.Sprintf("%s-cc", rc.triple),
		"XCXX=" + fmt.Sprintf("%s-c++", rc.triple),
	}
	if len(rc.ldflags) > 0 {
		args = append(args, fmt.Sprintf("-extldflags \"%s\"", strings.Join(rc.ldflags, " ")))
	}
	for _, name := range []string{"GOFLAGS", "TAGS", "SUFFIX"} {
		if value, ok := os.LookupEnv(name); ok {
			args = append(args, name+"="+value)
		}
	}
	args = append(args, os.Args[2:]...)

	cmd := exec.Command("make", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	const gopathKey = "GOPATH"
	if gopath, ok := os.LookupEnv(gopathKey); ok {
		cmd.Env = append(cmd.Env, gopathKey+"="+gopath)
	}
	cmd.Env = append(cmd.Env, "CGO_ENABLED=1")

	{
		sysroot := filepath.Join(xtoolsBase, rc.triple, "bin")
		if _, err := os.Stat(sysroot); err != nil {
			return errors.Wrapf(err, "os.Stat(%s)", sysroot)
		}
		const pathKey = "PATH"
		newPaths := []string{sysroot}
		if pathList, ok := os.LookupEnv(pathKey); ok {
			newPaths = append(newPaths, filepath.SplitList(pathList)...)
		}
		newPathList := strings.Join(newPaths, string(filepath.ListSeparator))
		cmd.Env = append(cmd.Env, pathKey+"="+newPathList)
	}

	var printCmd []string
	for _, env := range cmd.Env {
		printCmd = append(printCmd, shellEscape(env))
	}
	for _, arg := range cmd.Args {
		printCmd = append(printCmd, shellEscape(arg))
	}
	fmt.Fprintln(os.Stderr, strings.Join(printCmd, " "))
	return cmd.Run()
}

func shellEscape(s string) string {
	if strings.ContainsAny(s, " \t\n|&;()<>") {
		return fmt.Sprintf("\"%s\"", strings.Replace(s, "\"", "\\\"", -1))
	}
	return s
}
