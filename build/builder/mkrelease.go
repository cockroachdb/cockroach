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
//
//   - linux-gnu:  amd64, Linux 2.6.32, dynamically link glibc 2.12.2
//   - linux-musl: amd64, Linux 2.6.32, statically link musl 1.1.16
//   - linux-msan: amd64, recent Linux, enable Clang's memory sanitizer
//   - linux-arm:  arm64, Linux 3.7.10, dynamically link glibc 2.12.2
//   - darwin:     amd64, macOS 10.9
//   - windows:    amd64, Windows 8, statically link all non-Windows libraries
//
// These targets must be kept in sync with the crosstool-ng toolchains installed
// in the Dockerfile.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type releaseConfig struct {
	os      string
	arch    string
	triple  string
	cflags  string
	ldflags string
	extras  []string
}

var releaseConfigs = map[string]releaseConfig{
	"linux-gnu": {
		os:      "linux",
		arch:    "amd64",
		triple:  "x86_64-unknown-linux-gnu",
		ldflags: "-static-libgcc -static-libstdc++",
	},
	"linux-musl": {
		os:      "linux",
		arch:    "amd64",
		triple:  "x86_64-unknown-linux-musl",
		ldflags: "-static",
	},
	"linux-arm": {
		os:      "linux",
		arch:    "arm64",
		triple:  "aarch64-unknown-linux-gnueabi",
		ldflags: "-static-libgcc -static-libstdc++",
	},
	"linux-msan": {
		cflags:  "-fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -I/libcxx_msan/include -I/libcxx_msan/include/c++/v1",
		ldflags: "-fsanitize=memory -stdlib=libc++ -L/libcxx_msan/lib -lc++abi -Wl,-rpath,/libcxx_msan/lib",
		extras: []string{
			"GOFLAGS=-msan",
			"TAGS=stdmalloc",
		},
	},
	"darwin": {
		os:     "darwin",
		arch:   "amd64",
		triple: "x86_64-apple-darwin13",
		extras: []string{"EXTRA_XCMAKE_FLAGS=-DCMAKE_INSTALL_NAME_TOOL=x86_64-apple-darwin13-install_name_tool"},
	},
	"windows": {
		os:     "windows",
		arch:   "amd64",
		triple: "x86_64-w64-mingw32",
	},
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: %s RELEASE-CONFIG [MAKE-GOALS...]", os.Args[0])
	}

	rc, ok := releaseConfigs[os.Args[1]]
	if !ok {
		return fmt.Errorf("unknown release config %q", os.Args[1])
	}

	env := []string{"CGO_ENABLED=1"}
	if rc.ldflags != "" {
		env = append(env, "LDFLAGS="+rc.ldflags)
	}
	if rc.cflags != "" {
		env = append(env, "CFLAGS="+rc.cflags, "CXXFLAGS="+rc.cflags)
	}

	args := []string{"BUILDTYPE=release"}
	if rc.triple != "" {
		args = append(args,
			"TARGET_TRIPLE="+rc.triple,
			"XCMAKE_SYSTEM_NAME="+strings.Title(rc.os),
			"XGOOS="+rc.os,
			"XGOARCH="+rc.arch,
			"XCC="+fmt.Sprintf("%s-cc", rc.triple),
			"XCXX="+fmt.Sprintf("%s-c++", rc.triple),
		)
	}
	args = append(args, rc.extras...)
	args = append(args, os.Args[2:]...)

	fmt.Println(cmdString(env, "make", args))

	cmd := exec.Command("make", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), env...)
	return cmd.Run()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// cmdString produces a string representation of a command invocation that can
// be used to invoke cmd with the same arguments and environment from the shell.
func cmdString(env []string, cmd string, args []string) string {
	var out []string
	for _, e := range env {
		out = append(out, shellEscape(e))
	}
	out = append(out, "make")
	for _, a := range args {
		out = append(out, shellEscape(a))
	}
	return strings.Join(out, " ")
}

// shellEscape returns an escaped version of s that is safe to pass to
// POSIX-like shells. The escaping algorithm is optimized for readability rather
// than protection against all possible shell-injection attacks. Do not use this
// function on untrusted input.
func shellEscape(s string) string {
	if strings.ContainsAny(s, " \t\n\\|&;()<>$*?[#~'\"`") {
		// If the input contains any shell metacharacters, wrap it in double-quotes,
		// then backslash-escape the individual characters that are still special
		// within double-quoted strings.
		return `"` + strings.NewReplacer(`$`, `\$`, `\`, `\\`, `"`, `\"`, "`", "\\`").Replace(s) + `"`
	}
	return s
}
